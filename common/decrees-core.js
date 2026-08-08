const Util = require("./common-util.js");
const Path = require('node:path');
const Basic = require("./storage/basic.js");
const Schedule = require("../storage/schedule");

const Decrees = {};

const Utils = Decrees.Utils = {};
const isString = (str) => {
    return typeof(str) === "string";
};
const isInteger = (n) => {
    return !(typeof(n) !== 'number' || isNaN(n) || (n % 1) !== 0);
};
Utils.args_isBoolean = (args) => {
    return !(!Array.isArray(args) || typeof(args[0]) !== 'boolean');
};
Utils.args_isString = (args) => {
    return !(!Array.isArray(args) || !isString(args[0]));
};
Utils.args_isInteger = (args) => {
    return !(!Array.isArray(args) || !isInteger(args[0]));
};
Utils.args_isPositiveInteger = (args) => {
    return Array.isArray(args) && isInteger(args[0]) && args[0] > 0;
};

Decrees.create = (name, commands) => {
    // [<command>, <args>, <author>, <time>]
    const handleCommand = (Env, line) => {
        let command = line[0];
        let args = line[1];

        if (typeof(commands[command]) !== 'function') {
            throw new Error("DECREE_UNSUPPORTED_COMMAND");
        }

        let outcome = commands[command](Env, args);
        return outcome;
    };

    const createLineHandler = (Env) => {
        const Log = Env.Log;

        let index = -1;

        return (err, line) => {
            index++;
            if (err) {
                // Log the error and bail out
                return void Log.error("DECREE_LINE_ERR", {
                    error: err.message,
                    index: index,
                    line: line,
                });
            }

            if (Array.isArray(line)) {
                try {
                    return handleCommand(Env, line);
                } catch (err2) {
                    return void Log.error("DECREE_COMMAND_ERR", {
                        error: err2.message,
                        index: index,
                        line: line,
                    });
                }
            }

            Log.error("DECREE_HANDLER_WEIRD_LINE", {
                line: line,
                index: index,
            });
        };
    };

    const loadRemote = (Env, decrees, cb) => {
        cb ||= () => {};
        if (!Array.isArray(decrees)) {
            return void cb('INVALID_DECREES');
        }
        decrees.forEach(line => {
            if (!Array.isArray(line)) { return; }
            try {
                handleCommand(Env, line);
            } catch {}
        });
        cb();
    };

    const load = (Env, _cb) => {
        Env.scheduleDecree ||= Schedule();

        const toSend = [];

        const cb = Util.once(Util.mkAsync((err) => {
            if (err && err.code !== 'ENOENT') {
                return void _cb(err);
            }
            _cb(void 0, toSend);
        }));

        Env.scheduleDecree.blocking('', (unblock) => {
            const done = Util.once(Util.both(cb, unblock));
            /*  The decree log is small, read once at boot and written only by
                storage:0, so it is read whole rather than streamed. An absent log
                is the normal state of a fresh instance and is not an error.  */
            const decreeName = Path.join(Env.paths.decree, name);
            Basic.read(Env, decreeName, (err, content) => {
                if (err) { return void done(err); }

                const handler = createLineHandler(Env);
                content.split('\n').forEach(text => {
                    if (!text) { return; }
                    let line;
                    let changed = false;
                    try {
                        line = JSON.parse(text);
                        changed = handler(void 0, line);
                    } catch (err) {
                        handler(err, text);
                    }
                    if (changed) { toSend.push(line); }
                });
                done();
            });
        });
    };

    const write = function (Env, decree, _cb) {
        var path = Path.join(Env.paths.decree, name);
        // normally set up by load() at boot, but a write must not depend on
        // having been preceded by one
        Env.scheduleDecree ||= Schedule();
        Env.scheduleDecree.ordered('', function (next) {
            var cb = Util.both(Util.mkAsync(_cb), next);
            // written through: losing a decree loses instance configuration
            Basic.append(Env, path, JSON.stringify(decree) + '\n', cb);
        });
    };

    return {
        handleCommand,
        loadRemote,
        load,
        write
    };
};

module.exports = Decrees;
