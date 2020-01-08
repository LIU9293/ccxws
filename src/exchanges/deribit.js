const semaphore = require("semaphore");
const moment = require("moment");
const BasicClient = require("../basic-client");
const Ticker = require("../ticker");
const Trade = require("../trade");

export default class DeribitClient extends BasicClient {
  /**
   * Implements deribit V2 API
   * https://docs.deribit.com/v2/#deribit-api-v2-0-0
   */
  constructor() {
    super("wss://www.deribit.com/ws/api/v2/public/subscribe", "Deribit");
    this.hasTickers = true;
    this.hasTrades = true;
    this.hasLevel2Snapshots = false;
    this.hasLevel2Updates = false;
  }

  _beforeConnect() {
    this._wss.prependListener("connected", this._resetSemaphore.bind(this));
  }

  _resetSemaphore() {
    this._sem = semaphore(5);
    this._hasSnapshot = new Set();
  }

  _sendSubTicker(remote_id) {
    this._sem.take(() => {
      this._wss.send(
        JSON.stringify({
          jsonrpc: "2.0",
          method: "public/subscribe",
          id: remote_id.indexOf('BTC') >= 0 ? 1 : 2,
          params: {
            channels: [`ticker.${remote_id}.raw`]
          }
        })
      );
    });
  }

  _sendUnsubTicker(remote_id) {
    this._sem.take(() => {
      this._wss.send(
        JSON.stringify({
          jsonrpc: "2.0",
          method: "public/unsubscribe",
          id: remote_id.indexOf('BTC') >= 0 ? 1 : 2,
          params: {
            channels: [`ticker.${remote_id}.raw`]
          }
        })
      );
    });
  }

  _sendSubTrades(remote_id) {
    this._sem.take(() => {
      this._wss.send(
        JSON.stringify({
          jsonrpc: "2.0",
          method: "public/subscribe",
          id: remote_id.indexOf('BTC') >= 0 ? 3 : 4,
          params: {
            channels: [`trades.${remote_id}.raw`]
          }
        })
      );
    });
  }

  _sendUnsubTrades(remote_id) {
    this._wss.send(
      this._wss.send(
        JSON.stringify({
          jsonrpc: "2.0",
          method: "public/subscribe",
          id: remote_id.indexOf('BTC') >= 0 ? 3 : 4,
          params: {
            channels: [`trades.${remote_id}.raw`]
          }
        })
      )
    );
  }

  _onMessage(raw) {
    // process JSON message
    try {
      const msg = JSON.parse(raw);
      this._processsMessage(msg);
    } catch (ex) {
      this.emit("error", ex);
    }
  }

  _processsMessage(msg) {
    // subscribe success result
    if (!msg.method) {
      this._sem.leave();
      return;
    }

    // tickers
    if (msg.params && msg.params.channel.indexOf('ticker') >= 0) {
      this._processTicker(msg.params.data);
      return;
    }

    // trades
    if (msg.params && msg.params.channel.indexOf('trades') >= 0) {
      this._processTrades(msg.params.data);
      return;
    }
  }

  /**
   * Process ticker messages in the format
    {
      timestamp: 1577644586796,
      stats: [Object],
      state: 'open',
      settlement_price: 7305.74,
      open_interest: 77672560,
      min_price: 7289.93,
      max_price: 7511.96,
      mark_price: 7401.32,
      last_price: 7400.5,
      instrument_name: 'BTC-PERPETUAL',
      index_price: 7398.67,
      funding_8h: 0.00003953,
      current_funding: 0,
      best_bid_price: 7400,
      best_bid_amount: 282170,
      best_ask_price: 7400.5,
      best_ask_amount: 1660
    }
   */
  _processTicker(data) {
    const remoteId = data.instrument_name;
    const market = this._tickerSubs.get(remoteId);
    if (!market) return;

    // construct and emit ticker
    const ticker = this._constructTicker(data, market);
    this.emit("ticker", ticker, market);
  }

  /**
   * Processes trade messages in the format
    [{
      trade_seq: 32738413,
      trade_id: '55303618',
      timestamp: 1577646162415,
      tick_direction: 3,
      price: 7416,
      instrument_name: 'BTC-PERPETUAL',
      index_price: 7412.57,
      direction: 'sell',
      amount: 10
    }]
   */
  _processTrades(data) {
    for (const datum of data) {
      // ensure market
      const remoteId = datum.instrument_name;
      const market = this._tradeSubs.get(remoteId);
      if (!market) continue;

      // construct and emit trade
      const trade = this._constructTrade(datum, market);
      this.emit("trade", trade, market);
    }
  }

  _constructTicker(data, market) {
    const {
      timestamp,
      settlement_price,
      open_interest, // eslint-disable-line
      min_price,
      max_price,
      mark_price, // eslint-disable-line
      last_price,
      index_price, // eslint-disable-line
      funding_8h, // eslint-disable-line
      current_funding, // eslint-disable-line
      best_bid_price,
      best_bid_amount,
      best_ask_price,
      best_ask_amount
    } = data;

    const change = parseFloat(last_price) - parseFloat(settlement_price);
    const changePercent = change / parseFloat(settlement_price);
    const ts = moment.utc(timestamp).valueOf();
    return new Ticker({
      exchange: "Deribit",
      base: market.base,
      quote: market.quote,
      timestamp: ts,
      last: last_price,
      open: settlement_price,
      high: max_price,
      low: min_price,
      volume: 0,
      change: change.toFixed(8),
      changePercent: changePercent.toFixed(2),
      bid: best_bid_price,
      bidVolume: best_bid_amount,
      ask: best_ask_price,
      askVolume: best_ask_amount,
    });
  }

  _constructTrade(datum, market) {
    const { price, direction, amount, timestamp, trade_id } = datum;
    const ts = moment.utc(timestamp).valueOf();

    return new Trade({
      exchange: "Deribit",
      base: market.base,
      quote: market.quote,
      tradeId: trade_id,
      side: direction,
      unix: ts,
      price,
      amount,
    });
  }
}
