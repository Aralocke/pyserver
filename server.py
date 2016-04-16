import argparse
from contextlib import closing
import errno
import logging
import os
from Queue import Empty as EmptyQueueError, Queue
import select
import socket
import sys
import time


if os.name == 'posix':
  import fcntl

  def close_on_exec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

else:
  def close_on_exec(fd):
    pass


def server(server, backlog=5, log=None, nodelay=True, timeout=10.0):
  if log is None:
    log = logging.getLogger('server.mainloop')

  server.settimeout(1.0)
  server.setblocking(False)

  try:
    server.listen(backlog)
  except socket.error:
    log.critical("Failed to listen on server socket")
    return False

  addr, port = server.getsockname()
  log.info("Successfully initialized server socket '%s:%d'",
           addr, port)

  inputs = [server]
  outputs = list()
  messages = dict()

  isRunning = True

  while isRunning:
    try:
      try:
        reads, writes, errors = select.select(inputs, outputs, inputs, 1.0)

        if not (reads or writes or errors):
          continue

        for input in reads:
          if input is server:
            client, address = input.accept()
            log.info("Accepted new connection from '%s:%d'",
                     address[0], address[1])
            close_on_exec(client)
            client.setblocking(0)
            inputs.append(client)
            messages[client] = Queue()
          else:
            data = input.recv(8192)
            if data:
              log.debug("Received '%d' bytes from '%s:%d'",
                  len(data), input.getpeername()[0], input.getpeername()[1])

              if data is 'stop' or str(data) == 'stop':
                messages[input].put('Shutdown request received')
                raise StopIteration('Server received shutdown command')

              if input not in outputs:
                outputs.append(input)
              messages[input].put(data)

            else:
              log.info("Closing connection from '%s:%d'",
                  input.getpeername()[0], input.getpeername()[1])
              if input in outputs:
                outputs.remove(input)
              inputs.remove(input)
              input.close()
              del messages[input]

        for output in writes:
          if output not in messages:
            continue
          try:
            message = messages[output].get_nowait()
          except EmptyQueueError:
            outputs.remove(output)
          else:
            log.debug("Sending '%d' bytes to '%s:%d'", len(message),
                output.getpeername()[0], output.getpeername()[1])
            output.send(message)

        for error in errors:
          log.error("Socket '%s:%d' raised exception an condition",
              error.getpeername(), error.getpeername()[1])
          if error in outputs:
            outputs.remove(error)
          inputs.remove(error)
          error.close()
          del messages[error]
      except select.error as e:
        if e.args[0] not in [errno.EAGAIN, errno.EINTR]:
          raise
      except OSError as e:
        if e.errno not in [errno.EAGAIN, errno.EINTR]:
          raise
    except (KeyboardInterrupt, StopIteration):
      log.info('Received shutdown signal')
      for client in inputs:
        if client is server:
          continue
        try:
          if client in messages:
            while True:
              try:
                message = messages[client].get_nowait()
                client.send(message)
              except EmptyQueueError:
                break
            time.sleep(0.1)
        except socket.error:
          pass
        finally:
          client.close()
      log.info('All remaining sockets cleaned up successfully')
      return True
    except Exception:
      log.exception('Unhandled exception in main loop')
      return False

  return True

def main(host, port, nodelay=True):
  logger = logging.getLogger()

  try:
    sock_info = socket.getaddrinfo(host, port, socket.AF_UNSPEC,
        socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
  except socket.gaierror:
    if ':' in host:
      sock_info = [(socket.AF_INET6, socket.SOCK_STREAM, 0, '',
                    (host, port, 0, 0))]
    else:
      sock_info = [(socket.AF_INET, socket.SOCK_STREAM, 0, '',
                    (host, port))]

  logger.info("Initializing socket '%s:%d'", host, port)

  sock = None
  for (family, socktype, protocol, _, addr) in sock_info:
    try:
      sock = socket.socket(family, socktype, protocol)
      close_on_exec(sock)

      sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      if nodelay:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

      if family == socket.AF_INET6:
        if host in ('::', '::0', '::0.0.0.0'):
          try:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
          except (AttributeError, socket.error):
            logger.warning("Attempted to use IPv6 bind address '%s' on a system"
                           " that does not support dual-stack TCP", host)
            raise
      sock.bind((addr[0], addr[1]))
    except socket.error as e:
      if e.errno == errno.EADDRINUSE:
        logger.debug("Failed to bind to '%s:%d' - address in use",
                     addr[0], addr[1])
      elif e.errno == errno.EADDRNOTAVAIL:
        logger.debug("Failed to bind to '%s:%d' - invalid address",
                     addr[0], addr[1])
      logger.warning("Failed to create socket: %s:%d", addr[0], addr[1])
      if sock:
        sock.close()
      sock = None
      continue
    break

  if not sock:
    logger.critical("Failed to create socket '%s:%d'", host, port)
    return False

  logger.info("Server socket created on '%s:%d'", host, port)

  options = dict()
  options.setdefault('backlog', 5)
  options.setdefault('log', logger)
  options.setdefault('nodelay', nodelay)
  options.setdefault('timeout', 10.0)

  with closing(sock):
    if not server(sock, **options):
      return False

  return True

if __name__ == '__main__':
  parser = argparse.ArgumentParser('server')
  parser.add_argument('--host', default='localhost')
  parser.add_argument('--port', '-p', default=8080, type=int)
  args = parser.parse_args()

  root = logging.getLogger()
  root.setLevel(logging.DEBUG)
  console = logging.StreamHandler(stream=sys.stdout)
  console.setLevel(logging.DEBUG)
  root.addHandler(console)

  if not main(args.host, args.port):
    sys.exit(1)
