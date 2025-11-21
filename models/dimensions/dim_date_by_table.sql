{{ create_dim_date_new(
    source_table=source('stg', 'bookings'),
    date_column='book_date',
    week_start_day='sunday'
) }}