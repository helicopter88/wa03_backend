require 'pg'
require_relative 'yahoo_rest'
class DatabaseQueries
  def initialize(dbname)
    @conn = PG.connect(dbname: dbname)
    @yr = YahooRest.new
  end

  def parse_tokens(tokens)
    case tokens[0]
      # TODO: potentially useless
      # check_instr symbol
      when 'check_instr'
        return "ci_#{tokens[1]}: #{check_instr tokens[1]}"
      # login user password
      when 'login'
        return "lg_#{tokens[1]}: #{check_user tokens[1], tokens[2]}"
      # insert_user username password name capital currency
      # capital is casted to float
      when 'insert_user'
        return "iu_#{tokens[1]}: #{insert_user tokens[1],
                                               tokens[2], tokens[3], tokens[4].to_f, tokens[5]}"
      # TODO: this table is potentially useless
      # insert_instr symbol name
      when 'insert_instr'
        return "ii_#{tokens[1]}: #{insert_instrument tokens[1], tokens[2]}"
      # insert_trans user symbol price amount "true for buy | false for sell"
      # price casted to float, amount to int
      when 'insert_trans'
        return "it_#{tokens[1]}: #{insert_trans tokens[1], tokens[2],
                                                tokens[3].to_f, tokens[4].to_i, tokens[5]}"
      # get_capital user
      when 'get_capital'
        return "gc_#{tokens[1]}: #{get_user_capital tokens[1]}"
      # get_buy_trans user
      when 'get_buy_trans'
        return "gb_#{tokens[1]}: #{get_buy_trans tokens[1]}"
      # update_user_cap user newcapital
      # newcapital casted to float
      when 'update_user_cap'
        return "uc_#{tokens[1]}: #{update_user_capital tokens[1], tokens[2].to_f}"
      # get_total user
      when 'get_total'
        return "tt_#{tokens[1]}: #{get_total tokens[1]}"
      when 'get_profit'
        return "tp_#{tokens[1]}: #{get_profit tokens[1]}"
      when 'get_upnl'
        return "pl_#{tokens[1]}: #{get_unrealised_pnl tokens[1]}"
      when 'get_currency'
        return "cr_#{tokens[1]}: #{get_account_currency tokens[1]}"
      when 'get_name'
        return "nm_#{tokens[1]}: #{get_name tokens[1]}"
      else
        return 'db: invalid action'
    end
  end

  # Check whether the instrument exists in the instrument table
  def check_instr(instr)
    @conn.exec("SELECT * 
		FROM instruments 
		WHERE instr_id = '#{instr}'").ntuples == 1
  end

  # Check whether the given user owns the given instrument
  # (it has an open position)
  def users_instr(user, instr)
    q = @conn.exec("SELECT amount 
		    FROM owned 
		    WHERE user_id = '#{user}' 
		      AND instr_id = '#{instr}'")
    return 0 if q.ntuples == 0
    q.getvalue(0, 0).to_i
  end

  # Check whether the given user exists in the database
  def check_user(user, psw)
    @conn.exec("SELECT *
		FROM users 
		WHERE user_id = '#{user}' 
		  AND pword = '#{psw}'").ntuples == 1
  end

  # Get the name of the given user
  def get_name(user)
    q = @conn.exec("SELECT *
		FROM users
		WHERE user_id = '#{user}'")
    return "" unless q.ntuples == 1
    q.getvalue(0, 5).to_s
  end

  def get_account_currency(user)
    q = @conn.exec("SELECT currency FROM users WHERE user_id = '#{user}'")
    return nil if q.ntuples == 0
    q.getvalue(0, 0).to_s
  end

  # Deletes an user from the users database, if it exists, otherwise returns false
  def delete_user(user, psw)
    if check_user(user, psw)
      @conn.exec("DELETE FROM users WHERE user_id = '#{user}' AND pword = '#{psw}'")
      return true
    end
    false
  end

  # Inserts a user into the database if we don't already have their record otherwise returns false
  def insert_user(user, psw, name, capital, currency)
    unless check_user(user, psw)
      @conn.exec("INSERT INTO users 
		VALUES('#{user}', '#{psw}', '#{capital}',
		 '#{capital}', '#{currency}', '#{name}')")
      return true
    end
    false
  end

  # Deletes an instrument from instruments, if it exists, otherwise returns false
  def delete_instrument(instr, name)
    if check_instr(instr, name)
      @conn.exec("DELETE FROM instruments WHERE intr_id = '#{instr}'")
      return true
    end
    false
  end

  # Inserts an instrument into the database if we don't already have its record
  def insert_instrument(instr, name)
    unless check_instr(instr, name)
      @conn.exec("INSERT INTO instruments 
		VALUES('#{instr}', '#{name}')")
      return true
    end
    false
  end

  # Returns the number of shares currently owned (for a given instrument)
  def current_amount(user, instr)
    curr = @conn.exec("SELECT amount
       		       FROM owned
		       WHERE user_id = '#{user}'
	 	  	 AND instr_id = '#{instr}'")
    return 0 if curr.ntuples == 0
    curr.getvalue(0, 0).to_i
  end

  # Perform a transaction according to its type. Updates tables accordingly
  def insert_trans(user, instr, price, amount, type)
    # type -> 't' = buy, 'f' = sell
    u_capital = get_user_capital(user)
    value = price * amount
    curr = current_amount(user, instr)
    currency = get_se(instr)
    acc_currency = get_account_currency(user)
    # If the transaction is made in a different currency than the one we have
    # the account in, we reject it (for now)
    return false unless currency == acc_currency
    if type == 't' && value <= u_capital
      insert_instrument(instr, get_name_instr(instr))
      @conn.transaction do |con|
        con.exec "UPDATE users
		      SET capital = #{u_capital - value} 
		      WHERE user_id = '#{user}'"
        con.exec "INSERT INTO trans
		      (user_id, instr_id, price, amount, type, time, currency)
      		      VALUES ('#{user}', '#{instr}','#{price}','#{amount}',
		      '#{type}', clock_timestamp(), '#{currency}')"
        con.exec "INSERT INTO owned VALUES
		      ('#{user}', '#{instr}', '#{amount}', '#{currency}')
      		      ON CONFLICT (user_id, instr_id)
		      DO UPDATE SET amount = #{curr + amount}"
      end
      return true
    elsif type == 'f' && curr >= amount &&
        @conn.transaction do |con|
          con.exec "INSERT INTO trans
		      (user_id, instr_id, price, amount, type, time)
     		      VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}',
		      '#{type}', clock_timestamp(), '#{currency}')"
          con.exec "UPDATE ONLY users SET capital = #{u_capital + value}
		      WHERE user_id = '#{user}'"
          con.exec "UPDATE ONLY owned SET amount = #{curr - amount}
		      WHERE user_id = '#{user}' AND instr_id = '#{instr}'"
          con.exec "DELETE FROM ONLY owned WHERE amount = 0"
        end
      return true
    end
    false
  end

  # Get user capital if the user exists
  def get_user_capital(user)
    q = @conn.exec("SELECT capital FROM users WHERE user_id = '#{user}'")
    return 0 if q.ntuples == 0
    '%.3f' % q.getvalue(0, 0).to_f
  end

  # Calculate the total profit by adding up
  # the user capital and the unrealised pnl
  def get_total(user)
    get_user_capital(user).to_f + get_unrealised_pnl(user).to_f
  end

  # Gets the initial capital of the user
  def get_initial_capital(user)
    q = @conn.exec("SELECT initial FROM users WHERE user_id = '#{user}'")
    return 0 if q.ntuples == 0
    '%.3f' % q.getvalue(0, 0).to_f
  end

  # Calculates the profit by deducting the inital investment from the total
  def get_profit(user)
    get_total(user).to_f - get_initial_capital(user).to_f
  end

  # Returns the unrealised pnl of the given user 
  def get_unrealised_pnl(user)
    upnl = 0
    # To get the upnl, we go over the list of owned instruments
    # and get the yahoo price (we request the bid price)
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    q.each do |row|
      upnl += @yr.request_bid(row['instr_id']).to_f * row['amount'].to_i
    end
    '%.3f' % upnl
  end

  # Return the sell/bid price from the Yahoo API
  def get_sell_price(symbol)
    @yr.request_bid(symbol)
  end

  # Return the buy/ask price from the Yahoo API
  def get_buy_price(symbol)
    @yr.request_ask(symbol)
  end

  # Get the official name of the instrument from the Yahoo API
  def get_name_instr(symbol)
    @yr.request_name(symbol).strip![1..-2].to_s
  end

  # Currently prints all the buys of a given user
  #TODO: make an array and return as JSON file
  def get_buy_trans(user)
    q = @conn.exec("SELECT *
		   FROM trans 
		   WHERE user_id = '#{user}'
		     AND type = 't'")

    buy_arr = Array.new
    puts "'USER' 'INSTR' 'AMOUNT' 'PRICE' 'TIME' 'CURRENCY'"
    q.each do |row|
      # buy_arr.push({:user => row['user_id']
      puts '%s %s %d %f %s %s'.format([row['user_id'], row['instr_id'],
                                       row['amount'], row['price'], row['time'], row['currency']])
    end
  end

  # Currently prints all the sells of a given user
  #TODO: make an array and return as JSON file
  def get_sell_trans(user)
    q = @conn.exec("SELECT *
		   FROM trans 
		   WHERE user_id = '#{user}'
		     AND type = 'f'")
    puts "'USER' 'INSTR' 'AMOUNT' 'PRICE', 'TIME', 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %f %s %s'.format([row['user_id'], row['instr_id'],
                                       row['amount'], row['price'], row['time'], row['currency']])
    end
  end

  # Prints the current open positions of the given user
  def get_current_instr(user)
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    puts "'USER' 'INSTR' 'AMOUNT', 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %s'.format([row['user_id'], row['instr_id'],
                                 row['amount'], row['currency']])
    end
  end

  # Updates the user capital with the new given capital
  def update_user_capital(user, capital)
    @conn.exec("UPDATE users SET capital = '#{capital}' WHERE user_id = '#{user}'")
  end

  # FOR TESTING ONLY: deletes all the transactions of a user (does not update c)
  def clean_transactions(user)
    @conn.exec("DELETE FROM ONLY trans WHERE user_id = '#{user}'")
  end

  # Gets the currency of the given instrument by checking its stock exchange
  # Currently only supports London Stock Exchange, NASDAQ SE and New York SE
  def get_se(instr)
    #default is GBP
    c = 'GBP'
    se = @yr.retrieve_se(instr)
    if se.include?('NMS') || se.include?('NYQ')
      c = 'USD'
    end
    c
  end

  def user_data
    q = @conn.exec('SELECT * FROM users')
    puts 'No registered users' if q.ntuples == 0
    return @user_data if defined? @user_data && Time.new.to_i - @leader_time > 600
    user_data = Array.new
    q.each do |row|
      name = row['name']
      user = row['user_id']
      upnl = get_unrealised_pnl(user)
      profit = get_profit(user)
      total = get_total(user)
      user_data.push({:user => name, :upnl => upnl,
                      :profit => profit, :total => total})
    end
    @user_data = user_data
    @leader_time = Time.new.to_i
    user_data
  end

  def upnl_leaderboard
    udata = user_data
    udata.sort_by { |h| h[:upnl] }.reverse!
  end

  def profit_leaderboard
    udata = user_data
    udata.sort_by { |h| h[:profit] }.reverse!
  end

  def total_leaderboard
    udata = user_data
    udata.sort_by { |h| h[:total] }.reverse!
  end

  def print_leaderboard(type)
    type.downcase!
    if type == 'total'
      l = total_leaderboard
    elsif type == 'profit'
      l = profit_leaderboard
    elsif type == 'upnl'
      l = upnl_leaderboard
    else
      puts "The type can be 'total', 'profit' or 'upnl'"
    end
    puts " User  |  Profit  |  upnl  |  Total "
    l.each do |row|
      puts "#{row[:user]} | #{row[:profit].round(3)} | " \
	  "#{row[:upnl].round(3)} | #{row[:total].round(3)}"
    end
    nil
  end

  private :update_user_capital, :clean_transactions
end
