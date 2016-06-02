#!/usr/bin/ruby
require 'rest-client'
require 'json'
class YahooRest
  def initialize
    @names = {}
    @bids = {}
    @asks = {}
    @ses = {}
  end

  def parse_tokens(tokens)
    case tokens[0]
    when 'req_name'
      return "name #{jsonify(tokens[1], request_name(tokens[1]))}"
    # ask_price symbol
    when 'ask_price'
      return "ask_price #{jsonify(tokens[1], request_ask(tokens[1]))}"
    # bid_price symbol
    when 'bid_price'
      return "bid_price #{jsonify(tokens[1], request_bid(tokens[1]))}"
    # exists symbol
    when 'exists'
      return "exists #{check_existance(tokens[1])}"
    else
      return 'Yahoo: invalid action'
    end
  end

  def jsonify(symbol, value)
    { sym: symbol, res: value }.to_json
  end

  # Some basic getters that allow us to easily fetch data from Yahoo finance
  def request_name(symbol)
    params = { s: symbol, f: 'n' }
    # Names have no expiration date, just cache them always
    # @names[symbol] = Hash.new {|h,k| h[k] = request(params)}
    @names.fetch(symbol) { |k| @names[k] = request(params).strip! }
  end

  def request_bid(symbol)
    params = { s: symbol, f: 'b' }
    se = retrieve_se(symbol)
    if !@bids[symbol].nil? && Time.new.to_i - @bids[symbol][:time] > 60
      @bids.delete(symbol)
    end
    val = @bids.fetch(symbol) { |k| @bids[k] = { res: request(params), time: Time.new.to_i } }
    return (val[:res].to_f / 100).round(4) if se.include? 'LSE'
    val[:res].to_f.round(4)
  end

  def request_ask(symbol)
    params = { s: symbol, f: 'a' }
    se = retrieve_se(symbol)
    if !@asks[symbol].nil? && Time.new.to_i - @asks[symbol][:time] > 60
      @asks.delete(symbol)
    end
    val = @asks.fetch(symbol) { |k| @asks[k] = { res: request(params), time: Time.new.to_i } }
    return (val[:res].to_f / 100).round(4) if se.include? 'LSE'
    val[:res].to_f.round(4)
  end

  def retrieve_se(symbol)
    params = { s: symbol, f: 'x' }
    @ses.fetch(symbol) { |k| @ses[k] = request(params).strip! }
  end

  def check_existance(symbol)
    response = request_name(symbol)
    # Yahoo Finance returns "N/A" when the symbol does not match anything found
    !(response.eql? "N/A\n")
  end

  private

  # Performs the actual rest request
  def rest_req(params)
    # We may have to do some exception handling in case the request does not succeed
    RestClient.get('http://download.finance.yahoo.com/d/quotes.csv',
                   params: params) do |response|
      return response
    end
  end

  # This may cache the results for up to 1 minute
  # We do not do any sanity check for params
  # As we hope that every method will pass us correct arguments
  def request(params)
    rest_req(params)
  end
end
