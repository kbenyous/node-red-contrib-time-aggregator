var simpleStatistics = require('simple-statistics');

module.exports = function(RED) {
    fu

    function TimeAggregator(config) {
        RED.nodes.createNode(this,config);
        var node = this;

        node.interval = function(messageTime, intervalUnit){
            var result = {};
            var from = new Date(messageTime);
            var to = new Date(messageTime);

            switch (intervalUnit){
                case 'M':
                    from.setDate(1);                
                    to.setFullYear(to.getFullYear, to.getMonth()+1, 0);
                case 'd':
                    from.setHours(0);
                    to.setHours(23);
                case 'h':
                    from.setMinutes(0);
                    to.setMinutes(59);
                case 'm':
                    from.setSeconds(0,0);
                    to.setSeconds(59,999);
            }
            result.from = from;
            result.to = to;

            return result;
        };
        node.aggregate = function (list) {
            var output;

            switch (config.aggregationType) {
                case "mean":
                    output = simpleStatistics.mean(list);
                    break;

                case "geometricMean":
                    output = simpleStatistics.geometricMean(list);
                    break;

                case "harmonicMean":
                    output = simpleStatistics.harmonicMean(list);
                    break;

                case "median":
                    output = simpleStatistics.median(list);
                    break;

                case "min":
                    output = simpleStatistics.min(list);
                    break;

                case "max":
                    output = simpleStatistics.max(list);
                    break;

                case "sum":
                    output = simpleStatistics.sumSimple(list);
                    break;
            }

            return output;
        }; 


        node.on('input', function(msg) {
            var messageData = msg.payload();
            var messageTime = msg.time || new Date();
            
            var currentDataStorename = "timeagg_current_values_"+msg.topic;
            var currentTimeIntervalStorename = "timeagg_current_time_"+msg.topic;
            var previousAggregatedDataStorename = "timeagg_previous_value_"+msg.topic;
            var context = this.context();
    
            var currentValues = context.get(currentDataStorename) || [];
            var currentTimeInterval = context.get(currentTimeIntervalStorename) || interval(messageTime, config.intervalUnit);
            var previousValue= 0;
            
            if (messageTime < currentTimeInterval.from){
                // Message received to late, discard
                msg.payload = [];
            }else if ( messageTime > currentTimeInterval.to ){
                // Start a new time range
                context.set(currentTimeIntervalStorename, node.interval(messageTime, config.intervalUnit));

                // Perform data aggregation
                // TODO
                previousValue= node.aggregate(currentValues);
                context.set(previousAggregatedDataStorename, previousValue);
                
                // Create new data table with the new point
                currentValues = [messageData];
                context.set(currentDataStorename,currentValues);

                msg.payload = [previousValue, messageData]
            }else{
                // The message is in the current range

                // Update data table
                currentValues.push(messageData);
                context.set(currentDataStorename,currentValues);

                msg.payload = [null, messageData]

            }
          

            node.send(msg);
        });
    }
    RED.nodes.registerType("time-aggregator",TimeAggregator);
}
