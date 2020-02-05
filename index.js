const config                                  = require('config');
const path                                    = require('path');
const AWS                                     = require('aws-sdk');
const { extendDeep, loadFileConfigs }         = config.util;
const ourConfigDir                            = path.join(__dirname, 'config');
const defaultConfig                           = loadFileConfigs(ourConfigDir);
const Tracer                                  = require('jaeger-tracer');
const opentracing                             = Tracer.opentracing;
const { Tags, FORMAT_TEXT_MAP, globalTracer } = opentracing;
const tracer                                  = globalTracer();

config = extendDeep(defaultConfig.config, config.aws);
AWS.config.update(
     (config.aws, {
        httpOptions: {
            agent: new https.Agent(config.httpOptions)
        }
    })
);

//Set for every queue a uninque limiter.
const getSpanFromArgs = (args) => {
    const parentSpan = _.find((arg) => arg.constructor.name === 'Span')(args);
    const newArgs    = _.pull(parentSpan)(args);
    return { newArgs, parentSpan };
};

const endSpan = (sendMessagePromise, span) =>
    sendMessagePromise.then(() => {
        span.finish();
    }).catch((error) => {
        span.setTag(Tags.ERROR, true);
        span.log({
            event         : 'error',
            'error.object': error,
        });
        span.finish();
    });

const sendMessageAsync = async (...args) => {
    const traceConfig             = {};
    const { newArgs, parentSpan } = getSpanFromArgs(args);
    args                          = newArgs;
    if (parentSpan) traceConfig.childOf = parentSpan;
    const params  = _.find((arg) => 1 + _.indexOf('QueueUrl', _.keysIn(arg)))(args);
    const span    = tracer.startSpan('type', traceConfig);
    const carrier = {};
    if (params) {
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_MESSAGING_PRODUCER);
        tracer.inject(span, FORMAT_TEXT_MAP, carrier);
        params.MessageAttributes = {
            'traceId': {
                DataType   : 'String',
                StringValue: carrier['uber-trace-id']
            }
        };
    }

    return endSpan(sqsClient.sendMessage(...args).promise(), span);
};

const sendMessageBatchAsync = async (...args) => {
    const traceConfig             = {};
    const { newArgs, parentSpan } = getSpanFromArgs(args);
    args                          = newArgs;
    if (parentSpan) traceConfig.childOf = parentSpan;
    const params  = _.find((arg) => 1 + _.indexOf('QueueUrl', _.keysIn(arg)))(args);
    const span    = tracer.startSpan('type', traceConfig);
    const carrier = {};
    if (params) {
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_MESSAGING_PRODUCER);
        tracer.inject(span, FORMAT_TEXT_MAP, carrier);
        _.each((entery) => {
            entery.MessageAttributes = {
                'traceId': {
                    DataType   : 'String',
                    StringValue: carrier['uber-trace-id']
                }
            };
        })(params.Entries);
    }
    return endSpan(sqsClient.sendMessageBatch(...args).promise(), span);
};

module.exports = {
    sendMessageAsync,
    sendMessageBatchAsync
};