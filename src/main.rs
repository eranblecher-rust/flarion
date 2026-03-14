use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::ScalarUDF;
use crate::regexp_extract::RegexpExtractUDF;
use datafusion::execution::FunctionRegistry;

mod regexp_kernel;
mod regexp_extract;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let regexp_extract_impl = RegexpExtractUDF::new();
    let regexp_extract_udf = ScalarUDF::from(regexp_extract_impl);

    ctx.register_udf(regexp_extract_udf);

    let sql = "SELECT regexp_extract('email: test@gmail.com', '(\\w+)@(\\w+\\.\\w+)', 2) as domain";
    let df_sql = ctx.sql(sql).await?;
    df_sql.show().await?;

    let df = ctx.read_empty()?;
    let df = df.select(vec![
        ctx.udf("regexp_extract")?.call(vec![
            lit("user_12345"),
            lit(r"(\d+)"),
            lit(1)
        ])
    ])?;
    df.show().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;
    use datafusion::error::Result;

    #[tokio::test]
    async fn test_regexp_extract_sql_style() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(RegexpExtractUDF::new()));

        let df = ctx.sql("SELECT regexp_extract('email: test@gmail.com', '(\\w+)@(\\w+\\.\\w+)', 2) as domain").await?;
        let batches = df.collect().await?;

        let str_arr = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "gmail.com");

        Ok(())
    }

    #[tokio::test]
    async fn test_regexp_extract_dataframe_api() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(RegexpExtractUDF::new()));

        let df = ctx.read_empty()?;
        let df = df.select(vec![
            ctx.udf("regexp_extract")?.call(vec![
                lit("user_12345"),
                lit(r"(\d+)"),
                lit(1)
            ])
        ])?;

        let batches = df.collect().await?;
        let str_arr = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "12345");

        Ok(())
    }
}
