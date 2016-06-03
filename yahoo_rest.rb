#!/usr/bin/ruby
require 'rest-client'
require 'json'

#
# Class used to fetch data from yahoo
# Note: results may be cached
#
class YahooRest
  def initialize
    @names = {}
    @bids = {}
    @asks = {}
    @ses = {}
  end

  def parse_tokens(tokens)
    action = tokens[0]
    symbol = tokens[1]
    "#{action}: #{jsonify(symbol, send(action, symbol))}"
  end

  def jsonify(symbol, value)
    { sym: symbol, res: value }.to_json
  end

  # Some basic getters that allow us to easily fetch data from Yahoo finance
  def req_name(symbol)
    params = { s: symbol, f: 'n' }
    # Names have no expiration date, just cache them always
    fetch_cached_data(@names, symbol, params)
  end

  def bid_price(symbol)
    params = { s: symbol, f: 'b' }
    get_price(@bids, symbol, params)
  end

  def ask_price(symbol)
    params = { s: symbol, f: 'a' }
    get_price(@asks, symbol, params)
  end

  def retrieve_se(symbol)
    params = { s: symbol, f: 'x' }
    fetch_cached_data(@ses, symbol, params)
  end

  def exists(symbol)
    response = req_name(symbol)
    # Yahoo Finance returns "N/A" when the symbol does not match anything found
    !(response.eql? "N/A\n")
  end

  private

  def fetch_cached_data(h, symbol, params)
    h.fetch(symbol) { |k| h[k] = request(params).strip! }
  end
  
  def get_price(h, symbol, params)
    se = retrieve_se(symbol)
    h.delete(symbol) if h[symbol] && Time.new.to_i - h[symbol][:time] > 60
    val = h.fetch(symbol) { |k| h[k] = { res: request(params), time: Time.new.to_i } }
    return (val[:res].to_f / 100).round(4) if se.include? 'LSE'
    val[:res].to_f.round(4)
  end

  # This may cache the results for up to 1 minute
  # We do not do any sanity check for params
  # As we hope that every method will pass us correct arguments
  def request(params)
    # We may have to do some exception handling in case the request does not succeed
    RestClient.get('http://download.finance.yahoo.com/d/quotes.csv',
                   params: params) do |response|
      return response
    end
  end
end
