from config_spark._spark_testcase import SparkTestCase
from data._data_mocks import make_df
from spark_transform.transform import normalize_yes_no


class TestNormalizeYesNo(SparkTestCase):
    def test_converts_known_tokens_and_handles_unknown(self):
        df = make_df(self.spark)
        # colonnes yes/no à convertir
        cols = ["SCC", "SMOKE", "family_history_with_overweight", "FAVC"]
        out = normalize_yes_no(df, cols)

        r1, r2 = out.orderBy("Age").collect()

        # Ligne 1 (TINY_ROWS: "no" -> False)
        self.assertIs(r1["SCC"], False)
        self.assertIs(r1["SMOKE"], False)
        self.assertIs(r1["family_history_with_overweight"], False)
        self.assertIs(r1["FAVC"], False)  # "no" -> False

        # Ligne 2 ("Frequently" n'est ni yes ni no => None)
        self.assertIs(r2["SCC"], False)
        self.assertIs(r2["SMOKE"], False)
        self.assertIs(r2["family_history_with_overweight"], False)
        self.assertIsNone(r2["FAVC"])

    def test_unexpected_tokens_become_none(self):
        # jeu minimal pour couvrir un token inattendu
        df = self.spark.createDataFrame([("MAYBE",)], ["SCC"])
        out = normalize_yes_no(df, ["SCC"])
        self.assertIsNone(out.first()["SCC"])
