```python
from pyspark.sql import SparkSession
from pyspark import SparkConf
from IPython.display import display
from pyspark.sql.functions import *
from pyspark.sql import DataFrame, Window
import pandas as pd

from spotify_secret import spark_config, hdfs_path

'''
Inicjacja sparkSession z odpowiednimi parametrami
'''
conf = SparkConf().setAll([(key, value) for key, value in spark_config.items()])

spark = SparkSession \
            .builder \
            .master('yarn') \
            .config(conf=conf) \
            .getOrCreate()
```


```python
'''
Do zrobienia: wyliczenie delty z "energy", "dancability" w kolejnych latach. 
Czyli dla roku n pokazujesz o ile zmienil sie wspolczynnik "energy"
i "dancebility" w stosunku do roku n-1. 
Kod wystaw proszę gdzies na gitlabie/githubie. Bonus points za testy.
'''
```


```python
'''
Wczytanie danych wejściowych do sparka: plik data_by_year.csv
'''
spotify_path = f'{hdfs_path}/data_by_year.csv'
spotify = spark.read.format('csv').options(header='true', inferSchema='true').load(spotify_path)


'''
Eksploracja zbioru, sprawdzenie czy struktura została poprawnie wczytana (typy zmiennych).
Do podglądu użyto pandasa bo ma lepsze wyświetlanie dataframów niż pyspark.
'''
spotify.printSchema()
spotify.limit(10).toPandas()
```

    root
     |-- year: integer (nullable = true)
     |-- acousticness: double (nullable = true)
     |-- danceability: double (nullable = true)
     |-- duration_ms: double (nullable = true)
     |-- energy: double (nullable = true)
     |-- instrumentalness: double (nullable = true)
     |-- liveness: double (nullable = true)
     |-- loudness: double (nullable = true)
     |-- speechiness: double (nullable = true)
     |-- tempo: double (nullable = true)
     |-- valence: double (nullable = true)
     |-- popularity: double (nullable = true)
     |-- key: integer (nullable = true)
     |-- mode: integer (nullable = true)
    





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>acousticness</th>
      <th>danceability</th>
      <th>duration_ms</th>
      <th>energy</th>
      <th>instrumentalness</th>
      <th>liveness</th>
      <th>loudness</th>
      <th>speechiness</th>
      <th>tempo</th>
      <th>valence</th>
      <th>popularity</th>
      <th>key</th>
      <th>mode</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1920</td>
      <td>0.631242</td>
      <td>0.515750</td>
      <td>238092.997135</td>
      <td>0.418700</td>
      <td>0.354219</td>
      <td>0.216049</td>
      <td>-12.654020</td>
      <td>0.082984</td>
      <td>113.226900</td>
      <td>0.498210</td>
      <td>0.610315</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1921</td>
      <td>0.862105</td>
      <td>0.432171</td>
      <td>257891.762821</td>
      <td>0.241136</td>
      <td>0.337158</td>
      <td>0.205219</td>
      <td>-16.811660</td>
      <td>0.078952</td>
      <td>102.425397</td>
      <td>0.378276</td>
      <td>0.391026</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1922</td>
      <td>0.828934</td>
      <td>0.575620</td>
      <td>140135.140496</td>
      <td>0.226173</td>
      <td>0.254776</td>
      <td>0.256662</td>
      <td>-20.840083</td>
      <td>0.464368</td>
      <td>100.033149</td>
      <td>0.571190</td>
      <td>0.090909</td>
      <td>5</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1923</td>
      <td>0.957247</td>
      <td>0.577341</td>
      <td>177942.362162</td>
      <td>0.262406</td>
      <td>0.371733</td>
      <td>0.227462</td>
      <td>-14.129211</td>
      <td>0.093949</td>
      <td>114.010730</td>
      <td>0.625492</td>
      <td>5.205405</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1924</td>
      <td>0.940200</td>
      <td>0.549894</td>
      <td>191046.707627</td>
      <td>0.344347</td>
      <td>0.581701</td>
      <td>0.235219</td>
      <td>-14.231343</td>
      <td>0.092089</td>
      <td>120.689572</td>
      <td>0.663725</td>
      <td>0.661017</td>
      <td>10</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1925</td>
      <td>0.962702</td>
      <td>0.574875</td>
      <td>184777.623656</td>
      <td>0.278086</td>
      <td>0.420196</td>
      <td>0.237207</td>
      <td>-14.165355</td>
      <td>0.111668</td>
      <td>115.479832</td>
      <td>0.621187</td>
      <td>2.670251</td>
      <td>5</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1926</td>
      <td>0.666387</td>
      <td>0.596915</td>
      <td>157836.349887</td>
      <td>0.210506</td>
      <td>0.336093</td>
      <td>0.232021</td>
      <td>-18.487423</td>
      <td>0.477919</td>
      <td>109.570972</td>
      <td>0.437094</td>
      <td>1.436418</td>
      <td>9</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1927</td>
      <td>0.915826</td>
      <td>0.655929</td>
      <td>176542.812000</td>
      <td>0.267902</td>
      <td>0.320992</td>
      <td>0.177387</td>
      <td>-15.235276</td>
      <td>0.261915</td>
      <td>112.663707</td>
      <td>0.669676</td>
      <td>0.684000</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1928</td>
      <td>0.939141</td>
      <td>0.534907</td>
      <td>214396.032157</td>
      <td>0.208856</td>
      <td>0.490955</td>
      <td>0.174657</td>
      <td>-17.131424</td>
      <td>0.158645</td>
      <td>106.749062</td>
      <td>0.497412</td>
      <td>1.453333</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1929</td>
      <td>0.601015</td>
      <td>0.647840</td>
      <td>168795.609884</td>
      <td>0.241908</td>
      <td>0.215405</td>
      <td>0.235977</td>
      <td>-16.533056</td>
      <td>0.490464</td>
      <td>110.926711</td>
      <td>0.636805</td>
      <td>0.314406</td>
      <td>7</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
'''
Selekcja odpowiednich kolumn: 'year', 'danceability', 'energy'
'''
spotify = spotify.select('year', 'danceability', 'energy')


'''
Przygotowanie funckji window - porządkuje zbiór danych malejąco, a następnie
iteruje po kolejnych parach kolumny 'year' i oblicza różnicę wartości
kolumn 'danceability' oraz 'energy' w stosunku do roku poprzedniego, dodając wyniki
do kolumn, kolejno: 'danceability_delta' oraz 'energy_delta'
'''
w = Window.orderBy(spotify.year.desc())

spotify = spotify \
                .withColumn('danceability_delta', spotify.danceability - lead('danceability').over(w)) \
                .withColumn('energy_delta', spotify.energy - lead('energy').over(w))

'''
Wyświetlenie pierwszych 10 krotek wynikowego dataframe
'''
spotify.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>danceability</th>
      <th>energy</th>
      <th>danceability_delta</th>
      <th>energy_delta</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2021</td>
      <td>0.652488</td>
      <td>0.578896</td>
      <td>0.047203</td>
      <td>-0.095352</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2020</td>
      <td>0.605285</td>
      <td>0.674247</td>
      <td>0.002184</td>
      <td>0.044466</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2019</td>
      <td>0.603101</td>
      <td>0.629781</td>
      <td>0.000371</td>
      <td>-0.029771</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018</td>
      <td>0.602731</td>
      <td>0.659552</td>
      <td>0.022256</td>
      <td>-0.026840</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2017</td>
      <td>0.580475</td>
      <td>0.686392</td>
      <td>-0.001107</td>
      <td>0.029845</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2016</td>
      <td>0.581583</td>
      <td>0.656547</td>
      <td>0.021128</td>
      <td>0.065268</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2015</td>
      <td>0.560455</td>
      <td>0.591279</td>
      <td>-0.025698</td>
      <td>-0.073499</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2014</td>
      <td>0.586153</td>
      <td>0.664778</td>
      <td>-0.007728</td>
      <td>-0.027817</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2013</td>
      <td>0.593881</td>
      <td>0.692595</td>
      <td>0.035039</td>
      <td>0.005531</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2012</td>
      <td>0.558841</td>
      <td>0.687064</td>
      <td>-0.018098</td>
      <td>0.029299</td>
    </tr>
  </tbody>
</table>
</div>




```python
'''
Zapis wyników do pojedynczego pliku csv
'''

spotify.coalesce(1).write.csv(f'{hdfs_path}/spotify.csv')

```
