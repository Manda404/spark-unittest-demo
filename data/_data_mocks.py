# tests/_data.py
from dataclasses import dataclass

@dataclass
class ObesityRow:
    Age: float
    Gender: str
    Height: float
    Weight: float
    FAVC: str
    FCVC: float
    NCP: float
    SCC: str
    SMOKE: str
    CH2O: float
    family_history_with_overweight: str
    FAF: float
    TUE: float
    CAEC: str
    CALC: float
    MTRANS: str
    NObeyesdad: str

# 👇 Un petit mock canonique
MOCK_ROWS = [
    ObesityRow(
        Age=21.0, Gender="  Female  ", Height=1.62, Weight=64.0,
        FAVC="no", FCVC=2.0, NCP=3.0, SCC="no", SMOKE="no", CH2O=2.0,
        family_history_with_overweight="no", FAF=2.0, TUE=1.0, CAEC=" Sometimes ",
        CALC=0.0, MTRANS=" Public_Transportation ", NObeyesdad=" Normal_Weight "
    ),
    ObesityRow(
        Age=27.0, Gender="Male", Height=1.80, Weight=87.0,
        FAVC="Frequently", FCVC=3.0, NCP=3.0, SCC="no", SMOKE="no", CH2O=2.0,
        family_history_with_overweight="no", FAF=2.0, TUE=0.0, CAEC="Sometimes",
        CALC=2.0, MTRANS="Walking", NObeyesdad="Overweight_Level_I"
    ),
]

def make_df(spark):
    """Créer un DataFrame Spark à partir de la dataclass ObesityRow."""
    return spark.createDataFrame(MOCK_ROWS)
