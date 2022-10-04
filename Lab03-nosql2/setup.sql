DROP TABLE IF EXISTS financial_transactions CASCADE;

CREATE TABLE financial_transactions (
  transaction_id serial primary key,
  vendor_number varchar(50) not null,
  transaction_amount varchar(50),
  transaction_type varchar(25)
);