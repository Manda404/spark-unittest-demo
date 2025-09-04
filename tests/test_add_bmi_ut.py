from config_spark._spark_testcase import SparkTestCase
from data._data_mocks import make_df
from spark_transform.transform import add_bmi


class TestAddBMI(SparkTestCase):
    def test_adds_bmi_when_cols_exist(self):
        df = make_df(self.spark)
        out = add_bmi(df, height_col="Height", weight_col="Weight")

        self.assertIn("BMI", out.columns)
        r = out.orderBy("Age").first()
        expected = r["Weight"] / (r["Height"] ** 2)
        self.assertAlmostEqual(r["BMI"], expected, places=12)

    def test_noop_if_missing_columns(self):
        # DataFrame sans "Weight"
        df = self.spark.createDataFrame([(1.70,)], ["Height"])
        out = add_bmi(df, "Height", "Weight")
        self.assertNotIn("BMI", out.columns)
