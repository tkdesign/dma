# dma
E-commerce data management web application in Python.

### About
This web application is designed to manage and visualize sales data in an online store. It includes the following modules:
- **Dashboard**: visual analysis of KPIs.
- **Reports**: create and view analytical reports.
- **Migration**: data migration from operational to analytical database.
- **Users**: user registration and authorization.

The application has two levels of authorization:
- Basic HTTP authentication.
- Registered user authentication.

The application database is organized into three tiers:
- the data source (a MariaDB database restored from an OLTP dump, e.g. using mysqldump and mysql import),
- an intermediate layer for the ETL process (in PostgreSQL),
- an analytical data warehouse (also in PostgreSQL).

In addition, the application uses a Redis database to implement the task queue.

The structure of the analytical database is based on the Data Mart model in a star schema, with fact and dimension tables.

### Requirements
- Python 3.12+
- PostgreSQL 17+
- Redis 7.4+
- MariaDB 10.4+

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Author: Petr Kovalenko