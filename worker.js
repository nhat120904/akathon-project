const { Worker } = require('worker_threads');

function runWorkerThread() {
  return new Promise((resolve, reject) => {
    const worker = new Worker('./worker_thread.js');

    worker.on('message', (message) => {
      resolve(message);
    });

    worker.on('error', (error) => {
      reject(error);
    });
  });
}

module.exports = runWorkerThread;