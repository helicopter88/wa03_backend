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
      when 'ask_price'
        ws.send yr.request_ask(tokens[1])
      when 'bid_price'
        ws.send yr.request_bid(tokens[1])
      when 'exists'
        ws.send yr.check_existance(tokens[1])
      
      when 'check_instr'
	ws.send db.check_instr tokens[1], tokens[2]      	
      when 'login'
        ws.send db.check_user tokens[1], tokens[2]
      when 'insert_user'
        ws.send db.insert_user tokens[1], tokens[2], tokens[3]

      when 'insert_instr'
	ws.send db.insert_instrument tokens[1], tokens[2]

      when 'insert_trans'
	ws.send db.insert_trans tokens[1], tokens[2], tokens[3], tokens[4], tokens[5]

      when 'get_capital'
	ws.send db.get_user_capital tokens[1], tokens[2]

      when 'get_buy_trans'
	ws.send db.get_buy_trans tokens[1]      
      else
        ws.send "Invalid Message: #{msg}"
      end
    end
  end
end
