from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.getOrCreate()


class Init:
    METADATA_CSV = ""
    switch = ""

    @classmethod
    def dfVar(cls):
        cls.dfVar = spark \
            .read.format('com.databricks.spark.csv') \
            .options(header='true', inferschema='true', nullValue='', delimiter=',', quote='"', escape='"') \
            .load(cls.METADATA_CSV)


def checkCategoriesForXPaths(colNm1, colNm2):
    if "JAM" in Init.switch:
        dfNonCounters = Init.dfCSV.where(~(col(colNm1).like("%Counter%")) & (col(colNm1) == "XpathRecipe") & (
                col(colNm1) == "XpathDynamicRecipe") & ~(col(colNm2).like("%Counter%"))).select(col(colNm1),
                                                                                                Init.dfCSV.XpathValue_mhit)
        cntCat2Expected = dfNonCounters.where(dfNonCounters.XpathValue_mhit.isNotNull()).count()
    else:
        dfNonCounters = Init.dfCSV.where(~(col(colNm1).like("%Counter%")) & ~(col(colNm2).like("%Counter%"))).select(
            col(colNm1), Init.dfCSV.XpathValue)
        cntCat2Expected = dfNonCounters.where(dfNonCounters.XpathValue.isNotNull()).count()

    if cntCat2Expected > 0:
        print("%s has XPaths mentioned for Non Counters" % colNm1)
        return False
    else:
        print("%s has XPaths mentioned only for Counters" % colNm1)
        return True


def checkCategories(colNm, expectedList):
    CategoriesLegacyValid = True
    DistinctValues = list(
        map(lambda x: str(x[colNm]), Init.dfCSV.select(col(colNm)).where(col(colNm).isNotNull()).distinct().collect()))

    if len(set(DistinctValues) - set(expectedList)) > 0:
        CategoriesLegacyValid = False

    return CategoriesLegacyValid


def checkLegacyFieldName2CategoryNullMatch(colNmLgcyFld, colNmCat):
    LegacyFld2CategoryNullMatch = False
    cntMismatch1 = Init.dfCSV.select(colNmLgcyFld, colNmCat).where(
        (col(colNmLgcyFld).isNull()) & col(colNmCat).isNotNull()).count()

    if cntMismatch1 == 0:
        LegacyFld2CategoryNullMatch = True

    return LegacyFld2CategoryNullMatch


def checkCategory2LegacyFieldNameNullMatch(colNmLgcyFld, colNmCat):
    Category2LegacyFldNullMatch = False
    cntMismatch2 = Init.dfCSV.select(colNmLgcyFld, colNmCat).where(
        (col(colNmLgcyFld).isNotNull()) & col(colNmCat).isNull()).count()

    if cntMismatch2 == 0:
        Category2LegacyFldNullMatch = True

    return Category2LegacyFldNullMatch

    # print("Comparison categories used to expected: %s | Comparison expectect to categories used: %s" % (cntCat2Expected, cntExpected2Cat))


def Ind_PFN(xmasRegex):
    if ("supply" in xmasRegex):
        xmasRegex = xmasRegex.split('_', 2)[2]
        return xmasRegex


Ind_PFN_udf = udf(Ind_PFN, StringType())


def SupplyToXpath_PASSED(x, y):
    try:
        temp_list = re.findall(r'"(.*?)"', y)
        if "supply" in x:
            if "supply_na" not in x:
                z = ""
                if ('Cyan' in temp_list) or ('C' in temp_list[0]):
                    z = 'C'
                if ('Black' in temp_list) or ('K' in temp_list[0]):
                    z = 'K'
                if ('Magenta' in temp_list) or ('M' in temp_list[0]):
                    z = 'M'
                if ('Yellow' in temp_list) or ('Y' in temp_list[0]):
                    z = 'Y'
                if ((x.split('_', 2)[1]).upper()) == z:
                    return True
                else:
                    return False
            else:
                if ((('Cyan' in temp_list) or ('C' in temp_list)) or (
                        ('Magenta' in temp_list) or ('M' in temp_list)) or (
                        ('Yellow' in temp_list) or ('Y' in temp_list)) or ('imageDrum' in temp_list)):
                    return True
                else:
                    return False
    except Exception as e:
        return False


SupplyToXpath_PASSED_udf = udf(SupplyToXpath_PASSED, BooleanType())


def SupplyField_PASSED(x, list2):
    K_string = 'supply_k_' + x
    C_string = 'supply_c_' + x
    Y_string = 'supply_y_' + x
    M_string = 'supply_m_' + x
    NA_string = 'supply_na_' + x

    temp_list = []
    na_list = []
    if K_string in list2:
        K_INDICATOR = True
    else:
        K_INDICATOR = False
        temp_list.append('K')
        na_list.append('K')
    if C_string in list2:
        C_INDICATOR = True
    else:
        C_INDICATOR = False
        temp_list.append('C')
    if Y_string in list2:
        Y_INDICATOR = True
    else:
        Y_INDICATOR = False
        temp_list.append('Y')
    if M_string in list2:
        M_INDICATOR = True
    else:
        M_INDICATOR = False
        temp_list.append('M')
    if NA_string in list2:
        NA_INDICATOR = True
    else:
        NA_INDICATOR = False
        na_list.append('NA')
    if K_INDICATOR and C_INDICATOR and Y_INDICATOR and M_INDICATOR:
        return "Has all Fields"
    elif K_INDICATOR and NA_INDICATOR:
        return "Has all Fields"
    else:
        if len(temp_list) <= 2:
            return "Missing Fields List --> " + str(temp_list)
        elif len(na_list) == 1:
            return "Missing Fields List --> " + str(na_list)
        else:
            return "Missing Fields List --> " + str(na_list.append(temp_list))
