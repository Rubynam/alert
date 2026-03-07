

create table user_alert(
   alert_id text,
   symbol text,
   source text,
   target_price decimal,
   condition text,  -- 'ABOVE', 'BELOW', 'CROSS_ABOVE', 'CROSS_BELOW'
   status text,  -- 'ENABLED', 'DISABLED', 'TRIGGERED', 'EXPIRED'
   frequency_condtion text, -- 'only once', 'per_day', 'always
   max_hits int,
   created_at timestamp,
   updated_at timestamp,
   PRIMARY KEY (alert_id)
)