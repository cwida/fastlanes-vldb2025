#!/usr/bin/env python3

import pandas as pd
from io import StringIO

def main():
    avx2 = """
    table_name,version,file_size,decompression_time_ms,n_repetition
    Arade,btrblocks,1106852,493.102,1000
    Bimbo,btrblocks,316689,237.294,1000
    CMSprovider,btrblocks,3228119,1722.75,1000
    CityMaxCapita,btrblocks,10287861,5531.92,1000
    CommonGovernment,btrblocks,2359544,1602.01,1000
    Corporations,btrblocks,4760816,1613.3,1000
    Eixo,btrblocks,5367408,3563.85,1000
    Euro2016,btrblocks,6460921,3079.79,1000
    Food,btrblocks,408701,180.641,1000
    Generico,btrblocks,812085,662.013,1000
    HashTags,btrblocks,25791683,14836.3,1000
    Hatred,btrblocks,10964791,5902.65,1000
    IGlocations1,btrblocks,1554260,469.2,1000
    MLB,btrblocks,2269924,1380.57,1000
    MedPayment1,btrblocks,4215751,1295.88,1000
    Medicare1,btrblocks,3689924,816.047,1000
    Motos,btrblocks,1678930,1416.13,1000
    MulheresMil,btrblocks,5492452,3618.8,1000
    NYC,btrblocks,4878508,2393.01,1000
    PanCreactomy1,btrblocks,3625986,1917.74,1000
    Physicians,btrblocks,4271918,1759.11,1000
    Provider,btrblocks,4164752,1246.74,1000
    RealEstate1,btrblocks,4662863,2465.74,1000
    Redfin1,btrblocks,9854435,1739.53,1000
    Rentabilidad,btrblocks,15879115,4847.08,1000
    Romance,btrblocks,10200568,6251.29,1000
    SalariesFrance,btrblocks,10266585,3389.23,1000
    TableroSistemaPenal,btrblocks,479957,594.146,1000
    Taxpayer,btrblocks,4231123,1198.53,1000
    Telco,btrblocks,18627364,16367.1,1000
    TrainsUK1,btrblocks,2881640,1397.5,1000
    TrainsUK2,btrblocks,3579676,2110.09,1000
    USCensus,btrblocks,3855610,3050.27,1000
    Uberlandia,btrblocks,5416644,3615.62,1000
    Wins,btrblocks,14009171,9278.97,1000
    YaleLanguages,btrblocks,1448818,1042.56,1000
    """

    df = pd.read_csv(StringIO(avx2))

    total_decompression_time_sec = df["decompression_time_ms"].sum() / 1000

    print(f"Total decompression time (in seconds): {total_decompression_time_sec:.2f}")

    avx512 = """
     table_name,version,file_size,decompression_time_ms,n_repetition
    Arade,btrblocks,1106852,491.058,1000
    Bimbo,btrblocks,316689,235.531,1000
    CMSprovider,btrblocks,3343850,1888.38,1000
    CityMaxCapita,btrblocks,10344412,5645.61,1000
    CommonGovernment,btrblocks,2352772,1581.25,1000
    Corporations,btrblocks,4744918,1568.2,1000
    Eixo,btrblocks,5375734,3587.68,1000
    Euro2016,btrblocks,6460921,3089.79,1000
    Food,btrblocks,413619,205.328,1000
    Generico,btrblocks,835711,632.313,1000
    HashTags,btrblocks,25774061,14660.9,1000
    Hatred,btrblocks,10959574,6003.13,1000
    IGlocations1,btrblocks,1530406,490.446,1000
    MLB,btrblocks,2271086,1361.78,1000
    MedPayment1,btrblocks,4233636,1562.58,1000
    Medicare1,btrblocks,3693258,843.19,1000
    Motos,btrblocks,1655508,1340.86,1000
    MulheresMil,btrblocks,5470250,3652.54,1000
    NYC,btrblocks,4912601,2454.66,1000
    PanCreactomy1,btrblocks,3670932,2292.3,1000
    Physicians,btrblocks,4260444,1729.98,1000
    Provider,btrblocks,4198691,1548.2,1000
    RealEstate1,btrblocks,4682182,2429.31,1000
    Redfin1,btrblocks,9344264,2320.09,1000
    Rentabilidad,btrblocks,15933648,5070.7,1000
    Romance,btrblocks,10199986,6404.49,1000
    SalariesFrance,btrblocks,10210402,3496.53,1000
    TableroSistemaPenal,btrblocks,458307,612.358,1000
    Taxpayer,btrblocks,4216135,1601.26,1000
    Telco,btrblocks,18713343,16266,1000
    TrainsUK1,btrblocks,2881640,1405.49,1000
    TrainsUK2,btrblocks,3579676,2105.1,1000
    USCensus,btrblocks,3866750,2909,1000
    Uberlandia,btrblocks,5445658,3585.26,1000
    Wins,btrblocks,14071456,9283.85,1000
    YaleLanguages,btrblocks,1452906,1070.29,1000
    """

    df = pd.read_csv(StringIO(avx512))

    total_decompression_time_sec = df["decompression_time_ms"].sum() / 1000

    print(f"Total decompression time (in seconds): {total_decompression_time_sec:.2f}")

if __name__ == "__main__":
    main()
