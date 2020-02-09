let config = require('config');
const path = require('path');
const https = require('https');
const AWS = require('aws-sdk');
const { extendDeep, loadFileConfigs } = config.util;
const ourConfigDir = path.join(__dirname, 'config');
const defaultConfig = loadFileConfigs(ourConfigDir);
const Tracer = require('jaeger-tracer');
const opentracing = Tracer.opentracing;
const { Tags, FORMAT_TEXT_MAP, globalTracer } = opentracing;
const tracer = globalTracer();
const _ = require('lodash/fp');

config = extendDeep(defaultConfig, config.aws);
const agent = config.agent;
delete config.agent;
AWS.config.update(
    (config, {
        httpOptions: {
            agent: new https.Agent(agent.httpOptions)
        }
    })
);

const sqsClient = new AWS.SQS();

//Set for every queue a uninque limiter.
const getSpanFromArgs = (args) => {
    const parentSpan = _.find((arg) => arg.constructor.name === 'Span')(args);
    const newArgs = _.pull(parentSpan)(args);
    return { newArgs, parentSpan };
};

const endSpan = (sendMessagePromise, span) =>
    sendMessagePromise.then(() => {
        span.finish();
    }).catch((error) => {
        span.setTag(Tags.ERROR, true);
        span.log({
            event: 'error',
            'error.object': error,
        });
        span.finish();
    });

const sendMessageAsync = async (...args) => {
    const traceConfig = {};
    const { newArgs, parentSpan } = getSpanFromArgs(args);
    args = newArgs;
    if (parentSpan) traceConfig.childOf = parentSpan;
    const params = _.find((arg) => 1 + _.indexOf('QueueUrl', _.keysIn(arg)))(args);
    const span = tracer.startSpan('type', traceConfig);
    const carrier = {};
    if (params) {
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_MESSAGING_PRODUCER);
        tracer.inject(span, FORMAT_TEXT_MAP, carrier);
        const messageAttributes = {
            'traceId': {
                DataType: 'String',
                StringValue: carrier['uber-trace-id']
            }
        };
        params.MessageAttributes = _.merge(params.MessageAttributes, messageAttributes);
    }

    return endSpan(sqsClient.sendMessage(...args).promise(), span);
};

const sendMessageBatchAsync = async (...args) => {
    const traceConfig = {};
    const { newArgs, parentSpan } = getSpanFromArgs(args);
    args = newArgs;
    if (parentSpan) traceConfig.childOf = parentSpan;
    const params = _.find((arg) => 1 + _.indexOf('QueueUrl', _.keysIn(arg)))(args);
    const span = tracer.startSpan('type', traceConfig);
    const carrier = {};
    if (params) {
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_MESSAGING_PRODUCER);
        tracer.inject(span, FORMAT_TEXT_MAP, carrier);
        _.each((entery) => {
            const messageAttributes = {
                'traceId': {
                    DataType: 'String',
                    StringValue: carrier['uber-trace-id']
                }
            };
            entery.MessageAttributes = _.merge(entery.MessageAttributes, messageAttributes);
        })(params.Entries);
    }
    return endSpan(sqsClient.sendMessageBatch(...args).promise(), span);
};

module.exports = {
    sendMessageAsync,
    sendMessageBatchAsync
};
