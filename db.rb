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
      # check_instr symbol name
    when 'check_instr'
      return "ci_#{tokens[1]}: #{check_instr tokens[1], tokens[2]}"
      # login user password
    when 'login'
      return "l_#{tokens[1]}: #{check_user tokens[1], tokens[2]}"
      # insert_user username password capital
      # capital is casted to float
    when 'insert_user'
      return "iu_#{tokens[1]}: #{insert_user tokens[1], tokens[2], tokens[3].to_f}"
      # TODO: this table is potentially useless
      # insert_instr symbol name
    when 'insert_instr'
      return "ii_#{tokens[1]}: #{insert_instrument tokens[1], tokens[2]}"
      # insert_trans user symbol price amount "true for buy | false for sell"
      # price casted to float, amount to int
    when 'insert_trans'
      return "it_#{tokens[1]}: #{insert_trans tokens[1], tokens[2], tokens[3].to_f, tokens[4].to_i, tokens[5]}"
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
      # get_total_profit user
    when 'get_total_profit'
      return "tp_#{tokens[1]}: #{get_total_profit tokens[1]}"
    else
      return 'db: invalid action'
    end
  end

  def check_instr(instr, name)
    @conn.exec("SELECT * FROM instruments WHERE instr_id = '#{instr}' AND name = '#{name}'").ntuples == 1
  end

  def users_instr(user, instr)
    q = @conn.exec("SELECT amount FROM owned WHERE user_id = '#{user}' AND instr_id = '#{instr}")
    return 0 if q.ntuples == 0
    q.getvalue(0, 0).to_i
  end

  def check_user(user, psw)
    @conn.exec("SELECT * FROM users WHERE user_id = '#{user}' AND pword = '#{psw}'").ntuples == 1
  end

  def get_account_currency(user)
    q = @conn.exec("SELECT currency FROM users WHERE user_id = '#{user}'")
    return nil if q.ntuples == 0
    q.getvalue(0, 0).to_s
  end

  def insert_user(user, psw, capital, currency)
    @conn.exec("INSERT INTO users VALUES('#{user}', '#{psw}', '#{capital}', '#{capital}', '#{currency}')") unless check_user(user, psw)
  end

  def insert_instrument(instr, name)
    @conn.exec("INSERT INTO instruments VALUES('#{instr}', '#{name}')") unless check_instr(instr, name)
  end

  def current_amount(user, instr)
    curr = @conn.exec("SELECT amount
			    FROM owned
			    WHERE user_id = '#{user}'
				AND instr_id = '#{instr}'")
    return 0 if curr.ntuples == 0
    curr.getvalue(0, 0).to_i
  end

  # type: true for buy, false for sell
  def insert_trans(user, instr, price, amount, type)
    u_capital = get_user_capital(user)
    value = price * amount
    curr = current_amount(user, instr)
    currency = get_se(instr)
    acc_currency = get_account_currency(user)
    return false unless currency == acc_currency
    if type == 't'
      insert_instrument(instr, get_name_instr(instr))
      if value <= u_capital
        @conn.exec(
          "start transaction;
      		 UPDATE users SET capital = #{u_capital - value} WHERE user_id = '#{user}';
      		 INSERT INTO trans (user_id, instr_id, price, amount, type, time, currency)
      			 VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}', '#{type}', clock_timestamp(), '#{currency}');
      		 INSERT INTO owned VALUES ('#{user}', '#{instr}', '#{amount}', '#{currency}')
      		 ON CONFLICT (user_id, instr_id) DO UPDATE SET amount = #{curr + amount};
      		 commit;"
        )
        return true
      end
      return false
    else
      if curr >= amount
        @conn.exec(
          "start transaction;
     		 INSERT INTO trans (user_id, instr_id, price, amount, type, time)
     			 VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}', '#{type}', clock_timestamp());
     		 UPDATE ONLY users SET capital = #{u_capital + value} WHERE user_id = '#{user}';
     		 UPDATE ONLY owned SET amount = #{curr - amount} WHERE user_id = '#{user}' AND instr_id = '#{instr}';
     		 DELETE FROM ONLY owned WHERE amount = 0;
     		commit;"
        )
        return true
      end
    end
    false
  end

  def get_user_capital(user)
    @conn.exec("SELECT capital FROM users WHERE user_id = '#{user}'").getvalue(0, 0).to_i
  end

  def get_total_profit(user)
    get_user_capital(user) + get_unrealised_pnl(user)
  end

  def get_unrealised_pnl(user)
    upnl = 0
    # To get the upnl, we go over the list of owned instruments and get the yahoo price
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    q.each do |row|
      upnl += @yr.request_bid(row['instr_id']).to_f * row['amount'].to_i
    end
    upnl
  end

  def get_sell_price(symbol)
    @yr.request_bid(symbol)
  end

  def get_buy_price(symbol)
    @yr.request_ask(symbol)
  end

  def get_name_instr(symbol)
    @yr.request_name(symbol).strip!
  end

  def get_buy_trans(user)
    # TODO: make an array of (user_id, instr_id, amount, price) and return as JSON file
    q = @conn.exec("SELECT * FROM trans WHERE user_id = '#{user}' AND type = 't'")
    puts "'USER' 'INSTR' 'AMOUNT' 'PRICE' 'TIME' 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %f %s %s'.format([row['user_id'], row['instr_id'], row['amount'], row['price'], row['time'], row['currency']])
    end
  end

  def get_sell_trans(user)
    # TODO: make an array of (user_id, instr_id, amount, price) and return as JSON file
    q = @conn.exec("SELECT * FROM trans WHERE user_id = '#{user}' AND type = 'f'")
    puts "'USER' 'INSTR' 'AMOUNT' 'PRICE', 'TIME', 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %f %s %s'.format([row['user_id'], row['instr_id'], row['amount'], row['price'], row['time'], row['currency']])
    end
  end

  def get_current_instr(user)
    q = @conn.exec("SELECT * FROM owned WHERE user_id = '#{user}'")
    puts "'USER' 'INSTR' 'AMOUNT', 'CURRENCY'"
    q.each do |row|
      puts '%s %s %d %s'.format([row['user_id'], row['instr_id'], row['amount'], row['currency']])
    end
  end

  def update_user_capital(user, capital)
    @conn.exec("start transaction; UPDATE users SET capital = '#{capital}' WHERE user_id = '#{user}'; commit;")
  end

  def clean_transactions(user)
    @conn.exec("DELETE FROM ONLY trans WHERE user_id = '#{user}'")
  end

  def get_se(instr)
    se = @yr.retrieve_se(instr)
    if se.include? 'LSE'
      c = 'GBP'
    elsif se.include?('NMS') || se.include?('NYQ')
      c = 'USD'
    end
    c
  end
end
