use Test::Nginx::Socket::Lua;

plan tests => 3 * blocks();

no_shuffle();
run_tests();

__DATA__

=== TEST 1: Validate with default options
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
			}

			local ok, err = util.validate_options(opts)

			ngx.say(ok)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
--- no_error_log
[error]

=== TEST 2: Validate with custom options
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = "example.com",
				port = 12345,
				db = "testdb",
				hostname = "hostname.tld",
				proto = "udp",
				precision = "s",
				ssl = true
			}


			local ok, err = util.validate_options(opts)

			ngx.say(ok)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
true
--- no_error_log
[error]

=== TEST 3: Invalid host
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				host = true,
				db = "testdb"
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid host
--- no_error_log
[error]

=== TEST 4: Invalid port
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				port = true,
				db = "testdb"
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid port
--- no_error_log
[error]

=== TEST 5a: Invalid db
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				db = 1
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid db
--- no_error_log
[error]

=== TEST 5b: Empty db
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				db = ""
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid db
--- no_error_log
[error]

=== TEST 6: Invalid hostname
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				hostname = true
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid hostname
--- no_error_log
[error]

=== TEST 7: Invalid proto type
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				proto = 1
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid proto 1
--- no_error_log
[error]

=== TEST 8: Invalid proto value
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				proto = "foo"
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid proto foo
--- no_error_log
[error]

=== TEST 9: Invalid precision
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				precision = true
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid precision
--- no_error_log
[error]

=== TEST 10: Invalid ssl
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				ssl = "yes"
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid ssl
--- no_error_log
[error]

=== TEST 11: Invalid auth
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = {
				auth = true
			}

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
invalid auth
--- no_error_log
[error]

=== TEST 12: Invalid opts type
--- config
	location /t {
		content_by_lua '
			local util = require "resty.influx.util"

			local opts = true

			local ok, err = util.validate_options(opts)

			ngx.say(err)
		';
	}
--- request
GET /t
--- error_code: 200
--- response_body
opts must be a table
--- no_error_log
[error]

