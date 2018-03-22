var fs = require('fs');
var request = require('request');

var redis = require('redis');
var async = require('async');

var Stratum = require('stratum-pool');
var util = require('stratum-pool/lib/util.js');


module.exports = function (logger) {

    var poolConfigs = JSON.parse(process.env.pools);

    var enabledPools = [];

    Object.keys(poolConfigs).forEach(function (coin) {
        var poolOptions = poolConfigs[coin];
        if (poolOptions.paymentProcessing &&
            poolOptions.paymentProcessing.enabled)
            enabledPools.push(coin);
    });

    /*async.filter(enabledPools, function(coin, callback){
        SetupForPool(logger, poolConfigs[coin], function(setupResults){
            callback(setupResults);
        });
    }


    , function(coins){
        coins.forEach(function(coin){

            var poolOptions = poolConfigs[coin];
            var processingConfig = poolOptions.paymentProcessing;
            var logSystem = 'Payments';
            var logComponent = coin;

            logger.debug(logSystem, logComponent, 'Payment processing setup to run every '
                + processingConfig.paymentInterval + ' second(s) with daemon ('
                + processingConfig.daemon.user + '@' + processingConfig.daemon.host + ':' + processingConfig.daemon.port
                + ') and redis (' + poolOptions.redis.host + ':' + poolOptions.redis.port + ')');

        });
    }

    );*/

    async.filter(enabledPools, function (coin, callback) {
        SetupForPool(logger, poolConfigs[coin], function (setupResults) {
            callback(null, setupResults);
        });
    }, function (err, coins) {
        if (err) {
            console.log("Error processing enabled pools in the config"); // TODO: ASYNC LIB was updated, need to report a better error
        } else {
            coins.forEach(function (coin) {

                var poolOptions = poolConfigs[coin];
                var processingConfig = poolOptions.paymentProcessing;
                var logSystem = 'Payments';
                var logComponent = coin;

                logger.debug(logSystem, logComponent, 'Payment processing setup to run every '
                    + processingConfig.paymentInterval + ' second(s) with daemon ('
                    + processingConfig.daemon.user + '@' + processingConfig.daemon.host + ':' + processingConfig.daemon.port
                    + ') and redis (' + poolOptions.redis.host + ':' + poolOptions.redis.port + ')');

            });
        }
    });
};

function SetupForPool(logger, poolOptions, setupFinished) {

    var coin = poolOptions.coin.name;
    var processingConfig = poolOptions.paymentProcessing;
    var getMarketStats = poolOptions.coin.getMarketStats === true;

    var logSystem = 'Payments';
    var logComponent = coin;

    var daemon = new Stratum.daemon.interface([processingConfig.daemon], function (severity, message) {
        logger[severity](logSystem, logComponent, message);
    });
    var redisClient = redis.createClient(poolOptions.redis.port, poolOptions.redis.host);

    var magnitude;
    var minPaymentSatoshis;
    var coinPrecision;

    var paymentInterval;

    async.parallel([
        function (callback) {
            daemon.cmd('validateaddress', [poolOptions.address], function (result) {
                if (result.error) {
                    logger.error(logSystem, logComponent, 'Error with payment processing daemon ' + JSON.stringify(result.error));
                    callback(true);
                }
                else if (!result.response || !result.response.ismine) {
                    logger.error(logSystem, logComponent,
                        'Daemon does not own pool address - payment processing can not be done with this daemon, '
                        + JSON.stringify(result.response));
                    callback(true);
                }
                else {
                    callback()
                }
            }, true);
        },
        function (callback) {
            daemon.cmd('getbalance', [], function (result) {
                var wasICaught = false;
                if (result.error) {
                    callback(true);
                    return;
                }
                try {
                    var d = result.data.split('result":')[1].split(',')[0].split('.')[1];
                    magnitude = parseInt('10' + new Array(d.length).join('0'));
                    minPaymentSatoshis = parseInt(processingConfig.minimumPayment * magnitude);
                    coinPrecision = magnitude.toString().length - 1;
                }
                catch (e) {
                    logger.error(logSystem, logComponent, 'Error detecting number of satoshis in a coin, cannot do payment processing. Tried parsing: ' + result.data);
                    wasICaught = true;
                }
                finally {
                    if (wasICaught) {
                        callback(true);
                    } else {
                        callback();
                    }
                }

            }, true, true);
        }
    ], function (err) {
        if (err) {
            setupFinished(false);
            return;
        }
        paymentInterval = setInterval(function () {
            try {
                processPayments();
            } catch (e) {
                throw e;
            }
        }, processingConfig.paymentInterval * 1000);
        setTimeout(processPayments, 100);
        setupFinished(true);
    });

    var satoshisToCoins = function (satoshis) {
        return parseFloat((satoshis / magnitude).toFixed(coinPrecision));
    };

    var coinsToSatoshies = function (coins) {
        return coins * magnitude;
    };

    function cacheMarketStats() {
        var marketStatsUpdate = [];
        var coin = logComponent.replace('_testnet', '').toLowerCase();
        if (coin == 'zen')
            coin = 'zencash';
        
        request('https://api.coinmarketcap.com/v1/ticker/'+coin+'/', function (error, response, body) {
            if (error) {
                logger.error(logSystem, logComponent, 'Error with http request to https://api.coinmarketcap.com/ ' + JSON.stringify(error));
                return;
            }
            if (response && response.statusCode) {
                if (response.statusCode == 200) {
                    if (body) {
                        var data = JSON.parse(body);
                        if (data.length > 0) {
                            marketStatsUpdate.push(['hset', logComponent + ':stats', 'coinmarketcap', JSON.stringify(data)]);
                            redisClient.multi(marketStatsUpdate).exec(function(err, results){
                                if (err){
                                    logger.error(logSystem, logComponent, 'Error with redis during call to cacheMarketStats() ' + JSON.stringify(error));
                                    return;
                                }
                            });
                        }
                    }
                } else {
                    logger.error(logSystem, logComponent, 'Error, unexpected http status code during call to cacheMarketStats() ' + JSON.stringify(response.statusCode));
                }
            }
        });
    }

    function cacheNetworkStats () {
        var params = null;
        daemon.cmd('getmininginfo', params,
            function (result) {                
                if (!result || result.error || result[0].error || !result[0].response) {
                    logger.error(logSystem, logComponent, 'Error with RPC call getmininginfo '+JSON.stringify(result[0].error));
                    return;
                }
                
                var coin = logComponent;
                var finalRedisCommands = [];
                
                if (result[0].response.blocks !== null) {
                    finalRedisCommands.push(['hset', coin + ':stats', 'networkBlocks', result[0].response.blocks]);
                }
                if (result[0].response.difficulty !== null) {
                    finalRedisCommands.push(['hset', coin + ':stats', 'networkDiff', result[0].response.difficulty]);
                }
                if (result[0].response.networkhashps !== null) {
                    finalRedisCommands.push(['hset', coin + ':stats', 'networkSols', result[0].response.networkhashps]);
                }

                daemon.cmd('getnetworkinfo', params,
                    function (result) {
                        if (!result || result.error || result[0].error || !result[0].response) {
                            logger.error(logSystem, logComponent, 'Error with RPC call getnetworkinfo '+JSON.stringify(result[0].error));
                            return;
                        }
                        
                        if (result[0].response.connections !== null) {
                            finalRedisCommands.push(['hset', coin + ':stats', 'networkConnections', result[0].response.connections]);
                        }
                        if (result[0].response.version !== null) {
                            finalRedisCommands.push(['hset', coin + ':stats', 'networkVersion', result[0].response.version]);
                        }
                        if (result[0].response.subversion !== null) {
                            finalRedisCommands.push(['hset', coin + ':stats', 'networkSubVersion', result[0].response.subversion]);
                        }
                        if (result[0].response.protocolversion !== null) {
                            finalRedisCommands.push(['hset', coin + ':stats', 'networkProtocolVersion', result[0].response.protocolversion]);
                        }

                        if (finalRedisCommands.length <= 0)
                            return;

                        redisClient.multi(finalRedisCommands).exec(function(error, results){
                            if (error){
                                logger.error(logSystem, logComponent, 'Error with redis during call to cacheNetworkStats() ' + JSON.stringify(error));
                                return;
                            }
                        });
                    }
                );
            }
        );
    }


    // network stats caching every 58 seconds
    var stats_interval = 58 * 1000;
    var statsInterval = setInterval(function() {
        // update network stats using coin daemon
        cacheNetworkStats();
    }, stats_interval);
    
    // market stats caching every 5 minutes
    if (getMarketStats === true) {
        var market_stats_interval = 300 * 1000;
        var marketStatsInterval = setInterval(function() {
            // update market stats using coinmarketcap
            cacheMarketStats();
        }, market_stats_interval);
    }


    function roundTo(n, digits) {
        if (digits === undefined) {
            digits = 0;
        }
        var multiplicator = Math.pow(10, digits);
        n = parseFloat((n * multiplicator).toFixed(11));
        var test =(Math.round(n) / multiplicator);
        return +(test.toFixed(digits));
    }

    var satoshisToCoins = function(satoshis){
        return roundTo((satoshis / magnitude), coinPrecision);
    };

    var coinsToSatoshies = function(coins){
        return Math.round(coins * magnitude);
    };

    function coinsRound(number) {
        return roundTo(number, coinPrecision);
    }





    /* Deal with numbers in smallest possible units (satoshis) as much as possible. This greatly helps with accuracy
       when rounding and whatnot. When we are storing numbers for only humans to see, store in whole coin units. */

    var processPayments = function () {

        var startPaymentProcess = Date.now();

        var timeSpentRPC = 0;
        var timeSpentRedis = 0;

        var startTimeRedis;
        var startTimeRPC;

        var startRedisTimer = function () { startTimeRedis = Date.now() };
        var endRedisTimer = function () { timeSpentRedis += Date.now() - startTimeRedis };

        var startRPCTimer = function () { startTimeRPC = Date.now(); };
        var endRPCTimer = function () { timeSpentRPC += Date.now() - startTimeRedis };

        async.waterfall([

            /* Call redis to get an array of rounds - which are coinbase transactions and block heights from submitted
               blocks. */
            function (callback) {

                startRedisTimer();
                redisClient.multi([
                    ['hgetall', coin + ':balances'],
                    ['smembers', coin + ':blocksPending']
                ]).exec(function (error, results) {
                    endRedisTimer();

                    if (error) {
                        logger.error(logSystem, logComponent, 'Could not get blocks from redis ' + JSON.stringify(error));
                        callback(true);
                        return;
                    }



                    var workers = {};
                    for (var w in results[0]) {
                        workers[w] = { balance: coinsToSatoshies(parseFloat(results[0][w])) };
                    }

                    var rounds = results[1].map(function (r) {
                        var details = r.split(':');
                        return {
                            blockHash: details[0],
                            txHash: details[1],
                            height: details[2],
                            serialized: r
                        };
                    });

                    callback(null, workers, rounds);
                });
            },

            /* 
                Step 2 - check if mined block coinbase tx are ready for payment
                         * adds block reward to rounds object
                         * adds block confirmations count to rounds object
            */
            function (workers, rounds, callback) {

                var batchRPCcommand = rounds.map(function (r) {
                    return ['gettransaction', [r.txHash]];
                });

                batchRPCcommand.push(['getaccount', [poolOptions.address]]);

                startRPCTimer();
                daemon.batchCmd(batchRPCcommand, function (error, txDetails) {
                    endRPCTimer();

                    if (error || !txDetails) {
                        logger.error(logSystem, logComponent, 'Check finished - daemon rpc error with batch gettransactions '
                            + JSON.stringify(error));
                        callback(true);
                        return;
                    }

                    var addressAccount = "";

                    txDetails.forEach(function (tx, i) {

                        if (i === txDetails.length - 1) {
                            addressAccount = tx.result;
                            return;
                        }

                        var round = rounds[i];
                        
                        // update confirmations for round
                        if (tx && tx.result)
                            round.confirmations = parseInt((tx.result.confirmations || 0));

                        // look for transaction errors
                        if (tx.error && tx.error.code === -5) {
                            logger.warning(logSystem, logComponent, 'Daemon reports invalid transaction: ' + round.txHash);
                            round.category = 'kicked';
                            return;
                        }
                        else if (!tx.result.details || (tx.result.details && tx.result.details.length === 0)) {
                            logger.warning(logSystem, logComponent, 'Daemon reports no details for transaction: ' + round.txHash);
                            round.category = 'kicked';
                            return;
                        }
                        else if (tx.error || !tx.result) {
                            logger.error(logSystem, logComponent, 'Odd error with gettransaction ' + round.txHash + ' '
                                + JSON.stringify(tx));
                            return;
                        }
                        // get the coin base generation tx
                        var generationTx = tx.result.details.filter(function (tx) {
                            return tx.address === poolOptions.address;
                        })[0];


                        if (!generationTx && tx.result.details.length === 1) {
                            generationTx = tx.result.details[0];
                        }

                        if (!generationTx) {
                            logger.error(logSystem, logComponent, 'Missing output details to pool address for transaction '
                                + round.txHash);
                            return;
                        }
                        // get transaction category for round
                        round.category = generationTx.category;
                        // get reward for newly generated blocks
                        if (round.category === 'generate') {
                            round.reward = generationTx.amount || generationTx.value;
                        }
                    });

                    var canDeleteShares = function (r) {
                        for (var i = 0; i < rounds.length; i++) {
                            var compareR = rounds[i];
                            if ((compareR.height === r.height)
                                && (compareR.category !== 'kicked')
                                && (compareR.category !== 'orphan')
                                && (compareR.serialized !== r.serialized)) {
                                return false;
                            }
                        }
                        return true;
                    };

                    //Filter out orphaned / kicked blocks
                    rounds = rounds.filter(function (r) {
                        switch (r.category) {
                            case 'orphan':
                            case 'kicked':
                                r.canDeleteShares = canDeleteShares(r);
                            case 'generate':
                                return true;
                            case 'immature':
                                return true;
                            default:
                                return false;
                        }
                    });


                    callback(null, workers, rounds, addressAccount);

                });
            },


            /* Does a batch redis call to get shares contributed to each round. Then calculates the reward
               amount owned to each miner for each round. */
            function (workers, rounds, addressAccount, callback) {

                var shareLookups = rounds.map(function (r) {
                    return ['hgetall', coin + ':shares:round' + r.height]
                });

                startRedisTimer();
                redisClient.multi(shareLookups).exec(function (error, allWorkerShares) {
                    endRedisTimer();

                    if (error) {
                        callback('Check finished - redis error with multi get rounds share');
                        return;
                    }


                    rounds.forEach(function (round, i) {
                        var workerShares = allWorkerShares[i];

                        if (!workerShares) {
                            logger.error(logSystem, logComponent, 'No worker shares for round: '
                                + round.height + ' blockHash: ' + round.blockHash);
                            return;
                        }

                        for (var key in workerShares) {
                            if (workerShares.hasOwnProperty(key) && key.length !== 34) {
                                delete workerShares[key];
                            }
                        }
                        
                        switch (round.category) {
                            case 'kicked':
                            case 'orphan':
                                round.workerShares = workerShares;
                                break;

                            case 'generate':
                                /* We found a confirmed block! Now get the reward for it and calculate how much
                                   we owe each miner based on the shares they submitted during that block round. */
                                var reward = parseInt(round.reward * magnitude);

                                var totalShares = Object.keys(workerShares).reduce(function (p, c) {
                                    return p + parseFloat(workerShares[c])
                                }, 0);

                                for (var workerAddress in workerShares) {
                                    var percent = parseFloat(workerShares[workerAddress]) / totalShares;
                                    var workerRewardTotal = Math.floor(reward * percent);
                                    var worker = workers[workerAddress] = (workers[workerAddress] || {});
                                    worker.reward = (worker.reward || 0) + workerRewardTotal;
                                }
                                break;
                        }
                    });

                    callback(null, workers, rounds, addressAccount);
                });
            },


            /* Calculate if any payments are ready to be sent and trigger them sending
             Get balance different for each address and pass it along as object of latest balances such as
             {worker1: balance1, worker2, balance2}
             when deciding the sent balance, it the difference should be -1*amount they had in db,
             if not sending the balance, the differnce should be +(the amount they earned this round)
             */
            function(workers, rounds, addressAccount, callback) {

                var tries = 0;
                var trySend = function (withholdPercent) {
                    
                    var addressAmounts = {};
                    var balanceAmounts = {};
                    var shareAmounts = {};
                    var timePeriods = {};
                    var minerTotals = {};
                    var totalSent = 0;
                    var totalShares = 0;
                    
                    // track attempts made, calls to trySend...
                    tries++;
                    
                    // total up miner's balances
                    for (var w in workers) {
                        var worker = workers[w];
                        totalShares += (worker.totalShares || 0)
                        worker.balance = worker.balance || 0;
                        worker.reward = worker.reward || 0;
                        // get miner payout totals
                        var toSendSatoshis = Math.round((worker.balance + worker.reward) * (1 - withholdPercent));
                        var address = worker.address = (worker.address || getProperAddress(w.split('.')[0])).trim();
                        if (minerTotals[address] != null && minerTotals[address] > 0) {
                            minerTotals[address] += toSendSatoshis;
                        } else {
                            minerTotals[address] = toSendSatoshis;
                        }
                    }
                    // now process each workers balance, and pay the miner
                    for (var w in workers) {
                        var worker = workers[w];
                        worker.balance = worker.balance || 0;
                        worker.reward = worker.reward || 0;
                        var toSendSatoshis = Math.round((worker.balance + worker.reward) * (1 - withholdPercent));
                        var address = worker.address = (worker.address || getProperAddress(w.split('.')[0])).trim();
                        // if miners total is enough, go ahead and add this worker balance
                        if (minerTotals[address] >= minPaymentSatoshis) {
                            totalSent += toSendSatoshis;
                            // send funds
                            worker.sent = satoshisToCoins(toSendSatoshis);
                            worker.balanceChange = Math.min(worker.balance, toSendSatoshis) * -1;
                            if (addressAmounts[address] != null && addressAmounts[address] > 0) {
                                addressAmounts[address] = coinsRound(addressAmounts[address] + worker.sent);
                            } else {
                                addressAmounts[address] = worker.sent;
                            }
                        } else {
                            // add to balance, not enough minerals
                            worker.sent = 0;
                            worker.balanceChange = Math.max(toSendSatoshis - worker.balance, 0);
                            // track balance changes
                            if (worker.balanceChange > 0) {
                                if (balanceAmounts[address] != null && balanceAmounts[address] > 0) {
                                    balanceAmounts[address] = coinsRound(balanceAmounts[address] + satoshisToCoins(worker.balanceChange));
                                } else {
                                    balanceAmounts[address] = satoshisToCoins(worker.balanceChange);
                                }
                            }
                        }
                        // track share work
                        if (worker.totalShares > 0) {
                            if (shareAmounts[address] != null && shareAmounts[address] > 0) {
                                shareAmounts[address] += worker.totalShares;
                            } else {
                                shareAmounts[address] = worker.totalShares;
                            }
                        }
                    }

                    // if no payouts...continue to next set of callbacks
                    if (Object.keys(addressAmounts).length === 0){
                        callback(null, workers, rounds, []);
                        return;
                    }
                    
                    // do final rounding of payments per address
                    // this forces amounts to be valid (0.12345678)
                    for (var a in addressAmounts) {
                        addressAmounts[a] = coinsRound(addressAmounts[a]);
                    }

                    // POINT OF NO RETURN! GOOD LUCK!
                    // WE ARE SENDING PAYMENT CMD TO DAEMON
                    
                    // perform the sendmany operation .. addressAccount
                    var rpccallTracking = 'sendmany "" '+JSON.stringify(addressAmounts);
                    //console.log(rpccallTracking);

                    daemon.cmd('sendmany', [addressAccount || '', addressAmounts], function (result) {
                        // check for failed payments, there are many reasons
                        if (result.error && result.error.code === -6) {
                            // check if it is because we don't have enough funds
                            if (result.error.message && result.error.message.includes("insufficient funds")) {
                                // only try up to XX times (Max, 0.5%)
                                if (tries < 5) {
                                    // we thought we had enough funds to send payments, but apparently not...
                                    // try decreasing payments by a small percent to cover unexpected tx fees?
                                    var higherPercent = withholdPercent + 0.001; // 0.1%
                                    logger.warning(logSystem, logComponent, 'Insufficient funds (??) for payments ('+satoshisToCoins(totalSent)+'), decreasing rewards by ' + (higherPercent * 100).toFixed(1) + '% and retrying');
                                    trySend(higherPercent);
                                } else {
                                    logger.warning(logSystem, logComponent, rpccallTracking);
                                    logger.error(logSystem, logComponent, "Error sending payments, decreased rewards by too much!!!");
                                    callback(true);
                                }
                            } else {
                                // there was some fatal payment error?
                                logger.warning(logSystem, logComponent, rpccallTracking);
                                logger.error(logSystem, logComponent, 'Error sending payments ' + JSON.stringify(result.error));
                                // payment failed, prevent updates to redis
                                callback(true);
                            }
                            return;
                        }
                        else if (result.error && result.error.code === -5) {
                            // invalid address specified in addressAmounts array
                            logger.warning(logSystem, logComponent, rpccallTracking);
                            logger.error(logSystem, logComponent, 'Error sending payments ' + JSON.stringify(result.error));
                            // payment failed, prevent updates to redis
                            callback(true);
                            return;
                        }
                        else if (result.error && result.error.message != null) {
                            // invalid amount, others?
                            logger.warning(logSystem, logComponent, rpccallTracking);
                            logger.error(logSystem, logComponent, 'Error sending payments ' + JSON.stringify(result.error));
                            // payment failed, prevent updates to redis
                            callback(true);
                            return;
                        }
                        else if (result.error) {
                            // unknown error
                            logger.error(logSystem, logComponent, 'Error sending payments ' + JSON.stringify(result.error));
                            // payment failed, prevent updates to redis
                            callback(true);
                            return;
                        }
                        else {

                            // make sure sendmany gives us back a txid
                            var txid = null;
                            if (result.response) {
                                txid = result.response;
                            }
                            if (txid != null) {

                                // it worked, congrats on your pools payout ;)
                                logger.special(logSystem, logComponent, 'Sent ' + satoshisToCoins(totalSent)
                                    + ' to ' + Object.keys(addressAmounts).length + ' miners; txid: '+txid);

                                if (withholdPercent > 0) {
                                    logger.warning(logSystem, logComponent, 'Had to withhold ' + (withholdPercent * 100)
                                        + '% of reward from miners to cover transaction fees. '
                                        + 'Fund pool wallet with coins to prevent this from happening');
                                }

                                // save payments data to redis
                                var paymentBlocks = rounds.filter(function(r){ return r.category == 'generate'; }).map(function(r){
                                    return parseInt(r.height);
                                });
                                
                                var paymentsUpdate = [];
                                var paymentsData = {time:Date.now(), txid:txid, shares:totalShares, paid:satoshisToCoins(totalSent),  miners:Object.keys(addressAmounts).length, blocks: paymentBlocks, amounts: addressAmounts, balances: balanceAmounts, work:shareAmounts};
                                paymentsUpdate.push(['zadd', logComponent + ':payments', Date.now(), JSON.stringify(paymentsData)]);
                                
                                callback(null, workers, rounds, paymentsUpdate);

                            } else {

                                clearInterval(paymentInterval);

                                logger.error(logSystem, logComponent, 'Error RPC sendmany did not return txid '
                                    + JSON.stringify(result) + 'Disabling payment processing to prevent possible double-payouts.');

                                callback(true);
                                return;
                            }
                        }
                    }, true, true);
                };
                
                // attempt to send any owed payments
                trySend(0);
            },

            function (workers, rounds, paymentsUpdate, callback) {

                var totalPaid = 0;

                // var immatureUpdateCommands = [];
                var balanceUpdateCommands = [];
                var workerPayoutsCommand = [];

                // update worker paid/balance stats
                for (var w in workers) {
                    var worker = workers[w];
                    // update balances
                    if (worker.balanceChange !== 0) {
                        balanceUpdateCommands.push([
                            'hincrbyfloat',
                            coin + ':balances',
                            w,
                            satoshisToCoins(worker.balanceChange)
                        ]);
                    }
                    // update payouts
                    if (worker.sent !== 0) {
                        workerPayoutsCommand.push(['hincrbyfloat', coin + ':payouts', w, worker.sent]);
                        totalPaid += worker.sent;
                    }
                }

                var movePendingCommands = [];
                var roundsToDelete = [];
                var orphanMergeCommands = [];

                var confirmsUpdate = [];
                var confirmsToDelete = [];

                var moveSharesToCurrent = function (r) {
                    var workerShares = r.workerShares;
                    Object.keys(workerShares).forEach(function (worker) {
                        orphanMergeCommands.push(['hincrby', coin + ':shares:roundCurrent',
                            worker, workerShares[worker]]);
                    });
                };

                rounds.forEach(function (r) {

                    switch (r.category) {
                        case 'kicked':
                            movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksKicked', r.serialized]);
                        case 'orphan':
                            movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksOrphaned', r.serialized]);
                            if (r.canDeleteShares) {
                                moveSharesToCurrent(r);
                                roundsToDelete.push(coin + ':shares:round' + r.height);
                            }
                            return;
                        case 'generate':
                            confirmsToDelete.push(['hdel', coin + ':blocksPendingConfirms', r.blockHash]);
                            movePendingCommands.push(['smove', coin + ':blocksPending', coin + ':blocksConfirmed', r.serialized]);
                            roundsToDelete.push(coin + ':shares:round' + r.height);
                            return;
                        case 'immature':   
                            confirmsUpdate.push(['hset', coin + ':blocksPendingConfirms', r.blockHash, (r.confirmations || 0)]);
                            return;                               
                    }
                });

                var finalRedisCommands = [];

                if (movePendingCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(movePendingCommands);

                if (orphanMergeCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(orphanMergeCommands);

                if (confirmsUpdate.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(confirmsUpdate);
                
                if (confirmsToDelete.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(confirmsToDelete);

                if (balanceUpdateCommands.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(balanceUpdateCommands);

                if (workerPayoutsCommand.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(workerPayoutsCommand);

                if (roundsToDelete.length > 0)
                    finalRedisCommands.push(['del'].concat(roundsToDelete));

                if (paymentsUpdate.length > 0)
                    finalRedisCommands = finalRedisCommands.concat(paymentsUpdate)

                if (totalPaid !== 0)
                    finalRedisCommands.push(['hincrbyfloat', coin + ':stats', 'totalPaid', totalPaid]);

                if (finalRedisCommands.length === 0) {
                    callback();
                    return;
                }

                startRedisTimer();
                redisClient.multi(finalRedisCommands).exec(function (error, results) {
                    endRedisTimer();
                    if (error) {
                        clearInterval(paymentInterval);
                        logger.error(logSystem, logComponent,
                            'Payments sent but could not update redis. ' + JSON.stringify(error)
                            + ' Disabling payment processing to prevent possible double-payouts. The redis commands in '
                            + coin + '_finalRedisCommands.txt must be ran manually');
                        fs.writeFile(coin + '_finalRedisCommands.txt', JSON.stringify(finalRedisCommands), function (err) {
                            logger.error('Could not write finalRedisCommands.txt, you are fucked.');
                        });
                    }
                    callback();
                });
            }

        ], function () {


            var paymentProcessTime = Date.now() - startPaymentProcess;
            logger.debug(logSystem, logComponent, 'Finished interval - time spent: '
                + paymentProcessTime + 'ms total, ' + timeSpentRedis + 'ms redis, '
                + timeSpentRPC + 'ms daemon RPC');

        });
    };


    var getProperAddress = function (address) {
        if (address.length === 40) {
            return util.addressFromEx(poolOptions.address, address);
        }
        else return address;
    };


}
