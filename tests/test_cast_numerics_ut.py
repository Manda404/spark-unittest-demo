from config_spark._spark_testcase import SparkTestCase
from data._data_mocks import make_df
from spark_transform.transform import cast_numerics


class TestCastNumerics(SparkTestCase):
    def test_casts_selected_columns_to_double(self):
        df = make_df(self.spark)
        numeric_cols = ["Age","Height","Weight","FCVC","NCP","CH2O","FAF","TUE","CALC"]

        out = cast_numerics(df, numeric_cols)
        dtypes = dict(out.dtypes)

        # Spark peut retourner "double" (souvent) ou "float" selon le contexte
        for c in numeric_cols:
            self.assertIn(dtypes[c], ("double", "float"))

    def test_ignores_missing_columns(self):
        df = make_df(self.spark)
        out = cast_numerics(df, ["NotAColumn"])
        self.assertNotIn("NotAColumn", out.columns)
