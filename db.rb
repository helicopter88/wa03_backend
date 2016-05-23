require 'pg'
class DatabaseQueries
    def initialize(dbname)
        @conn = PG.connect(dbname: dbname)
    end
    
    def check_instr(instr, name)
    	return @conn.exec("SELECT * FROM instruments WHERE instr_id = '#{instr}' AND name = '#{name}'").ntuples == 1
    end

    def users_instr(instr, user)
	bought = @conn.exec("SELECT sum(amount) 
			     FROM trans 
			     WHERE user_id = '#{user}' 
				AND instr_id = '#{instr}'
				AND type = 't'"
			 ).getvalue(0,0).to_i
	sold = @conn.exec("SELECT sum(amount)
			   FROM trans
			   WHERE user_id = '#{user}'
				AND instr_id = '#{instr}'
				AND type = 'f'"
			).getvalue(0,0).to_i
    	return bought - sold
    end
    
    def check_user(user, psw)
    	return @conn.exec("SELECT * FROM users WHERE user_id = '#{user}' AND pword = '#{psw}'").ntuples == 1
    end

    def insert_user(user, psw, capital)
    	@conn.exec("INSERT INTO users VALUES('#{user}', '#{psw}', '#{capital}')") unless check_user(user, psw)
    end

    def insert_instrument(instr, name)
        @conn.exec("INSERT INTO intruments VALUES('#{instr}', '#{name}')") unless check_instr(instr, name)
    end
    
    # type: true for buy, false for sell
    def insert_trans(user, instr, price, amount, type)
    	u_capital = get_user_capital(user)
	value = price * amount
	if type == 't'
		if value <= u_capital	
		@conn.exec(
		"start transaction;
		 UPDATE users SET capital = '#{u_capital - value}' WHERE user_id = '#{user}';
		 INSERT INTO trans (user_id, instr_id, price, amount, type)
			 VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}', '#{type}');
		 commit;")
		else
		#TODO: display to the page
		puts "Not enough funds"
		end
	else
		if users_instr(instr, user) >= amount
		@conn.exec(
		"start transaction;
		 INSERT INTO trans (user_id, instr_id, price, amount, type)
			 VALUES ('#{user}', '#{instr}', '#{price}', '#{amount}', '#{type}');
		 UPDATE users SET capital = '#{u_capital + value}' WHERE users_id = '#{user}';
		 commit;")
		else
		#TODO: display to the page
		puts "Not enough stocks to sell"
		end
	end
    end

    def get_user_capital(user)
	return @conn.exec("SELECT capital FROM users WHERE user_id = '#{user}'").getvalue(0,0).to_i
    end

    def get_total_profit(user)
    end
    
    def get_buy_trans(user)
    end

    def update_user_capital(user, capital)
    	@conn.exec("start transaction; UPDATE users SET capital = '#{capital}' WHERE user_id = '#{user}'; commit;")
    end
end
