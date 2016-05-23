#!/usr/bin/ruby
require 'socket'
require_relative 'yahoo_rest'
require_relative 'db'
require 'em-websocket'

hostname = Socket.gethostname.strip
puts "Webserver is being hosted on #{hostname}" 
yr = YahooRest.new
db = DatabaseQueries.new 'summitdb'
EM.run do
  # for now use localhost, this will be changed later
  EM::WebSocket.run(host: '0.0.0.0', port:8080) do |ws|
    ws.onopen do |handshake|
      puts "User #{handshake.path} connected!"
      ws.send "Welcome #{handshake.path}"
    end

    ws.onclose { puts 'User disconnected' }

    ws.onmessage do |msg|
      puts msg
      msg.strip!
      # We want tokens separated by spaces
      tokens = msg.split
      # The first one is always going to be the action we want to execute
      case tokens[0]
      # ask_price symbol
      when 'ask_price'
        ws.send yr.request_ask(tokens[1])
      # bid_price symbol
      when 'bid_price'
        ws.send yr.request_bid(tokens[1])
      # exists symbol
      when 'exists'
        ws.send yr.check_existance(tokens[1])
      #
      # END OF YAHOO TOKENS
      #

      # TODO: potentially useless
      # check_instr symbol name
      when 'check_instr'
	ws.send db.check_instr tokens[1], tokens[2]      	
      # login user password
      when 'login'
        ws.send db.check_user tokens[1], tokens[2]
      # insert_user username password capital
      # capital is casted to float
      when 'insert_user'
        ws.send db.insert_user tokens[1], tokens[2], tokens[3].to_f
      # TODO: this table is potentially useless
      # insert_instr symbol name
      when 'insert_instr'
	ws.send db.insert_instrument tokens[1], tokens[2]
      # insert_trans user symbol price amount "true for buy | false for sell"
      # price casted to float, amount to int
      when 'insert_trans'
	ws.send db.insert_trans tokens[1], tokens[2], tokens[3].to_f, tokens[4].to_i, tokens[5]
      # get_capital user
      when 'get_capital'
	ws.send db.get_user_capital tokens[1]
      # get_buy_trans user
      when 'get_buy_trans'
	ws.send db.get_buy_trans tokens[1]
      # update_user_cap user newcapital
      # newcapital casted to float
      when 'update_user_cap'
	ws.send db.update_user_capital tokens[1], tokens[2].to_f
      # get_total_profit user
      when 'get_total_profit'
	ws.send db.get_total_profit tokens[1] 
      #
      # END OF DB TOKENS
      #
      else
        ws.send "Invalid Message: #{msg}"
      end
    end
  end
end
