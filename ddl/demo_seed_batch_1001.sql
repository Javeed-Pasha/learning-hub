INSERT OVERWRITE demo.staging.customer_source VALUES
    (1, 'Acme Retail', 'London', TIMESTAMP '2026-04-01 08:00:00'),
    (2, 'Northwind Stores', 'New York', TIMESTAMP '2026-04-01 08:15:00');

INSERT OVERWRITE demo.staging.orders_source VALUES
    (101, 1, 1001, 'PLACED', DECIMAL('250.00'), DATE '2026-04-01', TIMESTAMP '2026-04-01 09:00:00'),
    (102, 2, 1002, 'PLACED', DECIMAL('180.00'), DATE '2026-04-01', TIMESTAMP '2026-04-01 09:10:00');

INSERT OVERWRITE demo.staging.product_source VALUES
    (1001, 'Laptop Pro 15', 'Computing', DECIMAL('250.00'), TIMESTAMP '2026-04-01 07:30:00'),
    (1002, 'Noise Cancel Headset', 'Accessories', DECIMAL('180.00'), TIMESTAMP '2026-04-01 07:35:00');

INSERT OVERWRITE demo.staging.invoice_source VALUES
    (5001, 101, 'GENERATED', DECIMAL('250.00'), TIMESTAMP '2026-04-01 10:00:00'),
    (5002, 102, 'GENERATED', DECIMAL('180.00'), TIMESTAMP '2026-04-01 10:05:00');

INSERT OVERWRITE demo.staging.shipment_source VALUES
    (7001, 101, 'PICKED', 'LON-01', TIMESTAMP '2026-04-01 11:00:00'),
    (7002, 102, 'PICKED', 'NYC-01', TIMESTAMP '2026-04-01 11:05:00');