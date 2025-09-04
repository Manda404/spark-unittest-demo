from config_spark._spark_testcase import SparkTestCase
from data._data_mocks import make_df
from spark_transform.transform import trim_strings

class TestTrimStrings(SparkTestCase):
    def test_trim_strings_only_trims_string_columns(self):
        df = make_df(self.spark)               # DF à partir de la dataclass
        out = trim_strings(df)                  # action
        r = out.orderBy("Age").first()          # ordre déterministe

        # les string ont été trim
        self.assertEqual(r["Gender"], "Female")
        self.assertEqual(r["CAEC"], "Sometimes")
        self.assertEqual(r["MTRANS"], "Public_Transportation")
        self.assertEqual(r["NObeyesdad"], "Normal_Weight")

        # les numériques restent inchangés
        self.assertIsInstance(r["Age"], float)
