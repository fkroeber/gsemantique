## Tests

The following tests are designed in a way that they cover the common operations executed on spatio-temporal data cubes. They are tailored to assess the correctness of the scaling classes under a variety of...
* spatio-temporal extent objects (bbox, single and multi-polygon AoI)
* operations (reduce-over-space, reduce-over-time, groupby, concetenate, etc)
* output formats (merge, vrt, None) 

Not all combinations of parameters are possibly, e.g. virtual rasters (vrt) can't be build for non-spatial timeseries outputs. The set of combinations to be tested is defined within `test.py`.

Further remarks:
* The tests are not focussed on assessing the correct execution of semantique's core functionality, e.g. handling various dtypes (semantic categories vs. continuous, numerical values). 
* All tests are run based on Landsat data retrieved via Planetary Computer. 

### A. Spatio-temporal extents (located under tests/aois)

The following AoI inputs with different charateristics are procided to construct spatio-temporal as constraining elements and inputs for the recipe execution. In `test.py` these AoI will be used in two-ways: First, using their total enclosing bounding box as an input and, second, using their feature geometry as an input. 

<table>
  <tr>
    <th style="width: 20%">aoi name</th>
    <th style="width: 80%">characteristics</th>
  </tr>
  <tr>
    <td>polygon.geojson</td>
    <td>Single polygon covering a Norwegian island with a size of about 250km<sup>2</sup> and a bounding box of about 625km<sup>2</sup> (25km, 25km) (north-south, east-west).</td>
  </tr>
  <tr>
    <td>multipolygon.geojson</td>
    <td>Set of 4 polygons covering parts of New South Wales (Australia). The polygons are of varying sizes with the smallest one as tiny as (4m,4m), the two medium-sized ones with a size of (4.5km, 4.5km) and the largest one covering (25km, 25km). Together, they represent a total area of 550km<sup>2</sup>. The total bounding box covers an area of (45km, 35km) (north-south, east-west).</td>
  </tr>
  <tr>
    <td>point.geojson</td>
    <td>Single point located in the central part of Germany</td>
  </tr>
  <tr>
    <td>multipoint.geojson</td>
    <td>Set of 7 points scattered across the central part of Germany, covering a total bounding box of (125km, 200km) (north-south, east-west).</td>
  </tr>
</table> 

The EPSG codes for the UTM projections of each region are as follows:
* epsg 32754 - Australia
* epsg 32632 - Germany
* epsg 32634 - Norway 

### B. Test recipes (located under tests/recipes) 
The following test recipes with different characteristics are contained. They increase in their complexity such that executing them sequentially allows to debug the code in a systematic manner.

<table>
  <tr>
    <th style="width: 20%">recipe name</th>
    <th style="width: 40%">characteristics (operations)</th>
    <th style="width: 40%">type of outputs (number of results, shapes)</th>
  </tr>
  <tr>
    <td>01_treduce.json</td>
    <td>reduce over time</td>
    <td>single output, 2D array</td>
  </tr>
  <tr>
    <td>02_sreduce.json</td>
    <td>reduce over space</td>
    <td>single output, 1D array</td>
  </tr>
  <tr>
    <td>03_tconcatenated.json</td>
    <td>grouped by time, reduce over time, remerged by time</td>
    <td>single output, tD array with t number of times</td>
  </tr>
  <tr>
    <td>04_sconcatenated.json</td>
    <td>grouped by space, reduce over space, remerged by space</td>
    <td>single output, sD array with s number of features</td>
  </tr>
  <tr>
    <td>05_tgrouped.json</td>
    <td>grouped by time, reduce over time, omitted remerge</td>
    <td>single output, tD array with t number of times</td>
  </tr>
  <tr>
    <td>06_sgrouped.json</td>
    <td>grouped by space, reduce over space, omitted remerge</td>
    <td>single output, sD array with s number of features</td>
  </tr>
  <tr>
    <td>07_tmulti_grouped.json</td>
    <td>two recipe parts, multiple bands, grouped by time, reduce over time, omitted remerge</td>
    <td>two output, 1<sup>st</sup> one: 1D array, 2<sup>nd</sup> one: btD array with b number of bands and t number of times</td>
  </tr>
  <tr>
    <td>08_tmulti_double_strat.json</td>
    <td>two recipe parts, multiple bands, grouped by two time dims, reduce over time, remerge by time</td>
    <td>two output, 1<sup>st</sup> one: 1D array, 2<sup>nd</sup> one: btD array with b number of bands and t number of times</td>
  </tr>
  <tr>
    <td>09_udf.json</td>
    <td>reduce over time with user-defined function to cast dtypes & update NA values</td>
    <td>single output, 2D array</td>
  </tr>
</table> 
