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
const TWO_HOURS_IN_MS = 2 * 60 * 60 * 1000;

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

const getSpanFromArgs = (args) => {
    const parentSpan = _.find((arg) => arg.constructor.name === 'Span')(args);
    const newArgs = _.pull(parentSpan)(args);
    return { newArgs, parentSpan };
};

const timeoutPromise = (span) => new Promise((resolve) => {
    span.timeout = setTimeout(() => {
        resolve('span.timeout');
    }, TWO_HOURS_IN_MS);
});

const endSpan = async (sendMessagePromise, span) => {    
    try {
        const res = await Promise.race([sendMessagePromise, timeoutPromise(span)]);
        if (span.timeout) clearTimeout(span.timeout);
        if ('timeout' === res) span.setTag("span.timeout", true);
    }
    catch (error) {
        span.setTag(Tags.ERROR, true);
        span.log({
            event: 'error',
            'error.object': error,
        });
    }
    span.finish();
}

const sendMessageAsync = (...args) => {
    const traceConfig = {};
    const { newArgs, parentSpan } = getSpanFromArgs(args);
    args = newArgs;
    if (parentSpan) traceConfig.childOf = parentSpan;
    const params = _.find((arg) => 1 + _.indexOf('QueueUrl', _.keysIn(arg)))(args);
    const span = tracer.startSpan('sendMessage', traceConfig);
    const carrier = {};
    if (params) {
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_MESSAGING_PRODUCER);
        const queueUrl = _.getOr('missing', 'QueueUrl')(params);
        const messageGroupId = _.getOr('missing', 'MessageGroupId')(params);
        span.setTag("queue.address", queueUrl);
        span.setTag("queue.message.groupId", messageGroupId);
        try {
            const messageBody = JSON.parse(params.MessageBody);
            const messageType = _.getOr('missing', 'type')(messageBody);
            const messageContent = _.getOr('missing', 'content')(messageBody);
            span.setTag("queue.message.type", messageType);
            span.log({
                event: 'queue.message.content',
                value: messageContent,
            });
        }
        catch (error) { }
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

const sendMessageBatchAsync = (...args) => {
    const traceConfig = {};
    const { newArgs, parentSpan } = getSpanFromArgs(args);
    args = newArgs;
    if (parentSpan) traceConfig.childOf = parentSpan;
    const params = _.find((arg) => 1 + _.indexOf('QueueUrl', _.keysIn(arg)))(args);
    const span = tracer.startSpan('sendMessage', traceConfig);
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
