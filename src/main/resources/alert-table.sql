

create table user_alert(
   alert_id text not null,
   symbol text not null,
   trader_id text not null,
   metatrader_id text not null,
   user_group text not null,
   metatrader_group text not null,
   oms_server_id text not null,
   server_uuid varchar(255),
    server_id int,
    account_type varchar(50) not null ,
   target_price numberic(18,8),
   condition text,  -- 'ABOVE', 'BELOW', 'CROSS_ABOVE', 'CROSS_BELOW'
   status text,  -- 'ENABLED', 'DISABLED', 'TRIGGERED', 'EXPIRED', DISABLE_BY_BO
   frequency_condition text, -- 'only once', 'per_day', 'always
   max_hits int,
   hit_counts int,
   created_at timestamp,
   updated_at timestamp,
   PRIMARY KEY (alert_id)
)