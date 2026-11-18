import { Exec } from '@kubernetes/client-node'
import { EventEmitter } from 'events'
import { execPodStep } from '../src/k8s'

jest.mock('@kubernetes/client-node', () => ({
  KubeConfig: class {
    loadFromDefault() {}
    makeApiClient() {}
    getContexts() {
      return [{ namespace: 'default' }]
    }
  },
  Exec: class {
    exec() {}
  }
}))

class Socket extends EventEmitter {
  readonly OPEN = 1
  readyState = this.OPEN
  ping = jest.fn()
  terminate = jest.fn()
}

const exec = jest.spyOn(Exec.prototype, 'exec')

function startStep(socket = new Socket()) {
  let reportStatus: Parameters<Exec['exec']>[8]
  exec.mockImplementation(async (...args) => {
    reportStatus = args[8]
    return socket as Awaited<ReturnType<Exec['exec']>>
  })
  return {
    result: execPodStep(['true'], 'job', 'job'),
    socket,
    report: (status: 'Success' | 'Failure', message?: string) =>
      reportStatus?.({ status, message })
  }
}

test('a normal WebSocket close without command status fails the step', async () => {
  const { result, socket } = startStep()
  await Promise.resolve()
  socket.emit('close', 1000)
  await expect(result).rejects.toThrow('WebSocket closed (1000)')
})

test('a socket that closed before listeners were installed fails the step', async () => {
  const socket = new Socket()
  socket.readyState = 3
  const { result } = startStep(socket)
  await expect(result).rejects.toThrow('WebSocket is no longer open')
})

test('a transport error fails the step', async () => {
  const { result, socket } = startStep()
  await Promise.resolve()
  socket.emit('error', new Error('connection reset'))
  await expect(result).rejects.toThrow('connection reset')
  expect(socket.terminate).toHaveBeenCalled()
})

test('a Kubernetes Success status without a numeric code succeeds', async () => {
  const { result, report, socket } = startStep()
  await Promise.resolve()
  report('Success')
  socket.emit('close', 1000)
  await expect(result).resolves.toBeUndefined()
})

test('a heartbeat send error fails immediately without waiting for a pong timeout', async () => {
  jest.useFakeTimers()
  try {
    const { result, socket } = startStep()
    socket.ping.mockImplementation((_data, _mask, callback) => {
      callback(new Error('socket is closing'))
    })
    const rejected = expect(result).rejects.toThrow(
      'WebSocket ping failed: socket is closing'
    )
    await Promise.resolve()
    jest.advanceTimersByTime(5_000)
    await rejected
    expect(socket.terminate).toHaveBeenCalledTimes(1)
  } finally {
    jest.useRealTimers()
  }
})

test('a remote command failure retains its message', async () => {
  const { result, report, socket } = startStep()
  await Promise.resolve()
  report('Failure', 'command exited with code 1')
  socket.emit('close', 1000)
  await expect(result).rejects.toThrow('command exited with code 1')
})

test('a responsive quiet step stays alive, then fails when pongs stop', async () => {
  jest.useFakeTimers()
  try {
    const { result, socket } = startStep()
    const rejected = expect(result).rejects.toThrow('WebSocket heartbeat timed out')
    await Promise.resolve()
    for (let i = 0; i < 13; i++) {
      jest.advanceTimersByTime(5_000)
      socket.emit('pong')
    }
    expect(socket.ping).toHaveBeenCalledTimes(13)
    jest.advanceTimersByTime(55_000)
    expect(socket.terminate).not.toHaveBeenCalled()
    jest.advanceTimersByTime(5_000)
    await rejected
    expect(socket.terminate).toHaveBeenCalled()
  } finally {
    jest.useRealTimers()
  }
})
