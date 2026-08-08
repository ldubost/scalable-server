import * as Federation from './index.js';

process.on('message', (message: Message) => {
    Federation.start(message);
});

export const start = Federation.start;
