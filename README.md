# Pro Analytics 02 Python Starter Repository

> Use this repo to start a professional Python project.

- Additional information: <https://github.com/denisecase/pro-analytics-02>
- Project organization: [STRUCTURE](./STRUCTURE.md)
- Build professional skills:
  - **Environment Management**: Every project in isolation
  - **Code Quality**: Automated checks for fewer bugs
  - **Documentation**: Use modern project documentation tools
  - **Testing**: Prove your code works
  - **Version Control**: Collaborate professionally

---

## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.
Complete each step in the following guide and verify carefully.

- [SET UP MACHINE](./SET_UP_MACHINE.md)

---

## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template.
Complete each step in the following guide.

- [SET UP PROJECT](./SET_UP_PROJECT.md)

It includes the critical commands to set up your local environment (and activate it):

```shell
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**macOS / Linux / WSL:**

```shell
source .venv/bin/activate
```

---

## WORKFLOW 3. Daily Workflow

Please ensure that the prior steps have been verified before continuing.
When working on a project, we open just that project in VS Code.

### 3.1 Git Pull from GitHub

Always start with `git pull` to check for any changes made to the GitHub repo.

```shell
git pull
```

### 3.2 Run Checks as You Work

This mirrors real work where we typically:

1. Update dependencies (for security and compatibility).
2. Clean unused cached packages to free space.
3. Use `git add .` to stage all changes.
4. Run ruff and fix minor issues.
5. Update pre-commit periodically.
6. Run pre-commit quality checks on all code files (**twice if needed**, the first pass may fix things).
7. Run tests.

In VS Code, open your repository, then open a terminal (Terminal / New Terminal) and run the following commands one at a time to check the code.

```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```

NOTE: The second `git add .` ensures any automatic fixes made by Ruff or pre-commit are included before testing or committing.

</details>
</summary>Click to see a note on best practices</summary>

`uvx` runs the latest version of a tool in an isolated cache, outside the virtual environment.
This keeps the project light and simple, but behavior can change when the tool updates.
For fully reproducible results, or when you need to use the local `.venv`, use `uv run` instead.

</details>

### 3.3 Build Project Documentation

Make sure you have current doc dependencies, then build your docs, fix any errors, and serve them locally to test.

```shell
uv run mkdocs build --strict
uv run mkdocs serve
```

- After running the serve command, the local URL of the docs will be provided. To open the site, press **CTRL and click** the provided link (at the same time) to view the documentation. On a Mac, use **CMD and click**.
- Press **CTRL c** (at the same time) to stop the hosting process.

### 3.4 Execute

This project includes demo code.
Run the data_prep Python module to confirm everything is working.

In VS Code terminal, run:

```shell
uv run python -m analytics_project.data_prep
```

You should see:

- Log messages in the terminal
- Data files loaded and read
- DataFrame sizes

If this works, your project is ready! If not, check:

- Are you in the right folder? (All terminal commands are to be run from the root project folder.)
- Did you run the full `uv sync --extra dev --extra docs --upgrade` command?
- Are there any error messages? (ask for help with the exact error)

---

### 3.5 Git add-commit-push to GitHub

Anytime we make working changes to code is a good time to git add-commit-push to GitHub.

1. Stage your changes with git add.
2. Commit your changes with a useful message in quotes.
3. Push your work to GitHub.

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

This will trigger the GitHub Actions workflow and publish your documentation via GitHub Pages.

### 3.6 Modify and Debug

With a working version safe in GitHub, start making changes to the code.

Before starting a new session, remember to do a `git pull` and keep your tools updated.

Each time forward progress is made, remember to git add-commit-push.

### Project Commands

To run Python script

```shell
uv run python -m analytics_project.demo_module_basics
```

To run Jupyter Notebook

```shell
uv run jupyter lab
```

To run prepare_customers_data.py

```shell
python3 src/analytics_project/data_preparation/prepare_customers_data.py
```

To run prepare_products_data.py

```shell
python3 src/analytics_project/data_preparation/prepare_products_data.py
```

To run prepare_sales_data.py

```shell
python3 src/analytics_project/data_preparation/prepare_sales_data.py
```

To run data_scrubber.py

```shell
python3 src/analytics_project/data_scrubber.py
```

To run etl_to_dw.py

```shell
python3 src/analytics_project/etl_to_dw.py
```

### WORKFLOW 4. Data Warehouse design and ETL to DW

#### Fact table: Sales

| Column         | Type    |
|----------------|---------|
| sale_id        | INT PK  |
| date           | TEXT    |
| customer_id    | TEXT FK |
| product_id     | TEXT FK |
| store_id       | TEXT    |
| campaign_id    | TEXT    |
| quantity       | INT     |
| sales_amount   | REAL    |

#### Dimension Tables

#### Customers

| Column             | Type    |
|--------------------|---------|
| customer_id        | TEXT PK |
| name               | TEXT    |
| region             | TEXT    |
| join_date          | TEXT    |
| age                | INT     |
| subscription_status| TEXT    |

#### Products

| Column             | Type    |
|--------------------|---------|
| product_id         | TEXT PK |
| product_name       | TEXT    |
| category           | TEXT    |
| unit_price         | TEXT    |
| manufacture_year   | INT     |
| availability_status| TEXT    |

### WORKFLOW 5. Cross-Platform Reporting with Spark

Operating system: MacOS 11.7
Tool choices: Spark SQL to perform slice, dice, and drilldown operations.

---

#### Slice

Shows all products that are in stock.

```python
spark.sql("""
 SELECT *
    FROM product
    WHERE availability_status = 'In stock'
""").show()
```
<img width="1440" height="900" alt="Screen Shot 2025-11-30 at 4 27 06 PM" src="https://github.com/user-attachments/assets/a65da03a-aba6-4ea3-9712-75ff51307928" />

#### Dice

Shows the total sales in the North region by age.

```python
dice_df = spark.sql("""
    SELECT
        c.region,
        c.age,
        SUM(s.sales_amount) AS total_sales
    FROM sale s
    JOIN customer c
        ON s.customer_id = c.customer_id
    WHERE c.region = 'North'
    GROUP BY c.region, c.age
""")
dice_df.show(10)
```
<img width="1440" height="900" alt="Screen Shot 2025-11-30 at 4 28 01 PM" src="https://github.com/user-attachments/assets/9a48b5e4-b9bd-4506-8d44-051ce6c451bd" />


#### Drilldown

Shows the total sales by customer, region, and store

```python
drill_df = spark.sql("""
    SELECT
        c.region,
        s.store_id,
        SUM(s.sales_amount) AS total_sales,
        COUNT(s.sale_id) AS num_transactions
    FROM sale s
    JOIN customer c
        ON s.customer_id = c.customer_id
    GROUP BY c.region, s.store_id
    ORDER BY c.region, total_sales DESC
""")
drill_df.show(10)
```
<img width="1440" height="900" alt="Screen Shot 2025-11-30 at 4 28 18 PM" src="https://github.com/user-attachments/assets/37cf3820-904e-4118-bb9a-507c88710ff7" />

### WORKFLOW 6. OLAP

#### Business Goal

The business goal is to calculate the total revenue generated by each product category to understand which categories are driving the most revenue and which might need additional focus or promotion.

This question matters because total revenue by category is a key metric for prioritization, strategy, and growth decisions. This will help drive decisions on where to invest time and resources.

#### Data Source

This analysis was built on cleaned, prepared data.

#### Sales Table - Fact table

- `customer_id` for joining to the customers table
- `product_id` for joining to the products table
- `state` for geographic segmentation
- `sales_amount` for numeric summarization

#### Customers Table - Dimension table

- `customer_id` for joining to the sales table
- `region` for geographic segmentation
- `subscription_status` for subscription type

#### Products Table - Dimension table

- `product_id` for joining to the sales table
- `category` for grouping during drilldown aggregation

### Tools

The following tools were used for analysis:

- Spark (PySpark)
- SQL
- Python
  - Pandas
  - Matplotlib
- Jupyter Notebooks

### Workflow Logic

- Load data into Jupyter (done for sales, customers, and products tables)

```shell
sales_df = (
    spark.read.format("jdbc")
    .options(url=f"jdbc:sqlite:{dw_path}", dbtable="sales", driver="org.sqlite.JDBC")
    .load()
)
```

- Create temporary SQL views in Spark

```shell
sales_df.createOrReplaceTempView("sale")
customers_df.createOrReplaceTempView("customer")
products_df.createOrReplaceTempView("product")
```

- Slice aggregation: filter data by specific dimensions (product category)

```shell
slice_df = spark.sql("""
    SELECT *
    FROM product
    WHERE category = 'Electronics'
""")
slice_df.show(10)
```

- Dice aggregation: break down data into smaller dimensions

```shell
dice_df = spark.sql("""
    SELECT
        c.region AS region,
        c.subscription_status AS subscription_status,
        s.state AS state,
        SUM(s.sales_amount) AS total_revenue
    FROM sale s
    JOIN customer c
        ON s.customer_id = c.customer_id
    GROUP BY
        c.region,
        c.subscription_status,
        s.state
    ORDER BY
        c. region, c. subscription_status
""").toPandas()
print(dice_df.head(20))
```

- Drilldown aggregation: Explore data from general to specific levels

```shell
drilldown_df = spark.sql("""
    SELECT
        p.category,
        SUM(s.sales_amount) AS total_revenue
    FROM sale s
    JOIN product p
        ON s.product_id = p.product_id
    GROUP BY p.category
    ORDER BY total_revenue DESC
""")

drilldown_df.show()
```

### WORKFLOW 7. Custom BI Project

#### 1. The Business Goal

The objective of this project is to analyze customer segments in order to determine which group generates the greatest sales performance.

#### 2. Data Source

This analysis was built on cleaned, prepared data on sales, customers, and products.

#### 3. Tools Used

The following tools were used for analysis:

- Spark (PySpark)
- SQL
- Python
  - Pandas
  - Matplotlib
- Jupyter Notebooks

#### 4. Workflow & Logic

- Load data into Jupyter (done for sales, customers, and products tables)

```shell
sales_df = (
    spark.read.format("jdbc")
    .options(url=f"jdbc:sqlite:{dw_path}", dbtable="sales", driver="org.sqlite.JDBC")
    .load()
)
```

- Create temporary SQL views in Spark

```shell
sales_df.createOrReplaceTempView("sale")
customers_df.createOrReplaceTempView("customer")
products_df.createOrReplaceTempView("product")
```

- Slice aggregations: filter data by specific dimensions (customers by gender)

```shell
slice_df = spark.sql("""
    SELECT *
    FROM customer
    WHERE Gender = 'Male'
""")
slice_df.show(10)
```

```shell
slice_df = spark.sql("""
    SELECT *
    FROM customer
    WHERE Gender = 'Female'
""")
slice_df.show(10)
```

- Slice aggregation: filter data by specific dimensions (total revenue by state)

```shell
slice_df = spark.sql("""
    SELECT
        state,
        SUM(sales_amount) AS total_revenue
    FROM sale
    GROUP BY state
    ORDER BY total_revenue DESC
""")
slice_df.show(10)
```

- Dice aggregation: break down data into smaller dimensions

```shell
dice_df = spark.sql("""
    SELECT
        CASE
            WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
            WHEN c.age BETWEEN 26 AND 35 THEN '26-35'
            WHEN c.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN c.age > 50 THEN '51+'
            ELSE 'Unknown'
        END AS age_group,
        c.gender AS gender,
        SUM(s.sales_amount) AS total_revenue
    FROM sale s
    JOIN customer c
        ON s.customer_id = c.customer_id
    GROUP BY
        age_group,
        c.gender
    ORDER BY
        age_group,
        c.gender
""").toPandas()

print(dice_df.head(20))
```

- Drilldown aggregations: Explore data from general to specific levels

```shell
drilldown_age_state_df = spark.sql("""
    SELECT
        s.state AS state,
        CASE
            WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
            WHEN c.age BETWEEN 26 AND 35 THEN '26-35'
            WHEN c.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN c.age > 50 THEN '51+'
            ELSE 'Unknown'
        END AS age_group,
        SUM(s.sales_amount) AS total_revenue
    FROM sale s
    JOIN customer c
        ON s.customer_id = c.customer_id
    GROUP BY
        s.state,
        age_group
    ORDER BY
        SUM(s.sales_amount) DESC,   -- Most revenue states first
        s.state,
        age_group
""")

drilldown_age_state_df.show()
```

```shell
drilldown_age_gender_df = spark.sql("""
    SELECT
        CASE
            WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
            WHEN c.age BETWEEN 26 AND 35 THEN '26-35'
            WHEN c.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN c.age > 50 THEN '51+'
            ELSE 'Unknown'
        END AS age_group,
        c.Gender AS gender,
        SUM(s.sales_amount) AS total_revenue
    FROM sale s
    JOIN customer c
        ON s.customer_id = c.customer_id
    GROUP BY
        CASE
            WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
            WHEN c.age BETWEEN 26 AND 35 THEN '26-35'
            WHEN c.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN c.age > 50 THEN '51+'
            ELSE 'Unknown'
        END,
        c.Gender
    ORDER BY
        total_revenue DESC
""")

drilldown_age_gender_df.show()
```

```shell
drilldown_product_df = spark.sql("""
    SELECT
        p.category AS category,
        p.product_name AS product,
        SUM(s.sales_amount) AS total_revenue
    FROM sale s
    JOIN product p
        ON s.product_id = p.product_id
    GROUP BY
        p.category,
        p.product_name
    ORDER BY
        SUM(s.sales_amount) DESC,   -- highest revenue overall first
        p.category,
        p.product_name
""")

drilldown_product_df.show()
```

#### 5. Results

From this analysis, we can conclude that males generate more revenue than females across all segments. Washington is the highest-revenue state at $58,104.35, while Louisiana generates the least. Among the age groups (18–25, 26–35, 36–50, and 51+), the 51+ group contributes the most revenue, with males continuing to lead. In terms of product categories, “Doctors Offices” generates the highest revenue, while “Home – Yourself” generates the lowest.

<img width="546" height="397" alt="Screen Shot 2025-12-04 at 10 25 57 PM" src="https://github.com/user-attachments/assets/c256485e-b585-4ce2-a900-f5438ffea533" />

<img width="872" height="482" alt="Screen Shot 2025-12-04 at 10 26 45 PM" src="https://github.com/user-attachments/assets/a45cb63b-e27b-4d2b-b132-3b440d2d84d8" />

<img width="850" height="550" alt="Screen Shot 2025-12-04 at 10 27 09 PM" src="https://github.com/user-attachments/assets/435509a8-2e04-4312-abce-ff32f64613e6" />

<img width="1191" height="694" alt="Screen Shot 2025-12-04 at 10 27 37 PM" src="https://github.com/user-attachments/assets/e58f6129-9568-4e1f-995a-a4d8003bcf3f" />

<img width="768" height="491" alt="Screen Shot 2025-12-04 at 10 28 01 PM" src="https://github.com/user-attachments/assets/d1f72e4d-3f9f-4424-967f-84ebcc134e79" />

<img width="494" height="603" alt="Screen Shot 2025-12-04 at 10 28 44 PM" src="https://github.com/user-attachments/assets/25102901-620f-43c8-8f87-aac84721afbb" />


#### 6. Suggested Business Action

- Target high performing demographics more directly
  - Since we now know that males in the 51+ age group generate the most revenue, or that Washington brings in the highest revenue, we can aim promotions and market campaigns toward them.
- Improve performance in low revenue areas
  - We can analyze potential barriers in low revenue states such as Louisiana so we can start to introduce localized campaigns.
- Increase inventory, marketing, or deals for "Doctors Office" category of products
- Reevaluate low performing product categories
  - Consider updating product line, adjust pricing, and run target advertisements.

#### 7. Challenges

Some challenges I ran into during this project included figuring out the best visual aids, and having to rename the drilldown aggregations to have different names in order to create different visuals.

#### 8. Ethical Considerations

This analysis is based on sales transactions from a single day, rather than an extended timeframe. This may not reflect typical customer behavior and results could be misleading.
