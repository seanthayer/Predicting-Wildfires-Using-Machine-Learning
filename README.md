# Predicting Wildfires Using Machine Learning

![poster_img_captions_background](https://github.com/seanthayer/Predicting-Wildfires-Using-Machine-Learning/assets/72107207/fdb232e9-c7fd-4f6e-b141-e1a009090c8c)
**Figure 1**: A high-level illustration showing some of the considered factors, the discretization of Oregon, and a hypothetical final output of the model’s predictions

&nbsp;

*Predicting Wildfires Using Machine Learning* aims to accomplish exactly that: make use of machine learning algorithms, publicly available datasets, and an understanding of predominant factors behind wildfires to train a binary classification model capable of predicting wildfire risk with some probabilistic confidence across regions of the state of Oregon.

The primary motivations behind this project are to explore potential methods of quickly evaluating wildfire risk based on current and projected factors, and to act as an additional source of ready-to-use dataset aggregations.

## Current Status

Currently, the project has the following capabilities:

1. Automatic dataset retrievals and merging (with adjustable date range)
2. Discretization of Oregon state (with adjustable resolution)
3. Unit prediction using logistic regression

There are no known bugs, though certain areas may be a work in progress.

## Dependencies

All procedures are written in and were tested using Python 3.1. To run in its entirety, only the following three libraries should be necessary:

1. `numpy == 1.23.5`
2. `pandas == 1.5.3`
3. `matplotlib == 3.7.1`

Although, a few others were used during development. The easiest way to acquire all dependencies is by using the command `pip install -r requirements.txt` (see section *How to Use*).

## How to Use

Same major versions of Python 3 should function correctly when invoking the interpreter, so having any 3.x.x version of [Python](https://www.python.org/downloads/) installed will suffice. After installation and cloning of the repository, follow these steps:

1. Using your command line of choice (such as Git Bash), navigate to the repo's root directory on your machine
2. Run the command `pip install -r requirements.txt` to automatically fetch dependencies, or install dependencies individually
3. Dataset retrieval and discretization exist as stages in a pseudo pipeline. The easiest method of executing them in their entirety is by running the command `python run_pipeline.py` in the root directory, which will automatically run each stage in sequence. If you would prefer to run stages individually or run a specific stage, here is a simple dependency graph for the current process:

&nbsp;

<img src="https://github.com/seanthayer/Predicting-Wildfires-Using-Machine-Learning/assets/72107207/8793a758-a2fc-4036-9abb-8676831eae15" width="840px" height="265px"/>

**Figure 2**: A simple dependency graph showing pipeline stages and their prerequisites

&nbsp;

4. After the final dataset has been written to disk, running the command `python ./model/logistic_regression.py` will begin the training of a logistic regression model whose eventual output will include a "one-shot" accuracy (not in the typical neural network nomenclature sense) and a series of k-Fold cross validation accuracies (all accuracies also include standard deviations). The "one-shot" accuracy is merely the self-reported accuracy over the full dataset. This name will be changed to something more fitting in the future

## Credits

- OpenMeteo — Fine-grain climate data ([Link](https://open-meteo.com))
- National Interagency Fire Center (NIFC) — Wildland fire incident locations data ([Link](https://nifc.maps.arcgis.com/home/item.html?id=b4402f7887ca4ea9a6189443f220ef28))
- National Centers for Environmental Information (NCEI) — Coarse-grain climate data ([Link](https://www.ncei.noaa.gov/pub/data/cirs/climdiv/))

### Individuals

- Professor Stefan Lee, Oregon State University — Initial consultation on machine learning approaches
- Professor Alexander Ulbrich, Oregon State University — Initial advisement on data comprehension
- Professor Kirsten Winters, Oregon State University — Initial resources and wildfire factors assistance

## Citations

<details><summary>[Expand]</summary>
&nbsp;

- Hersbach, H., Bell, B., Berrisford, P., Biavati, G., Horányi, A., Muñoz Sabater, J., Nicolas, J., Peubey, C., Radu, R., Rozum, I., Schepers, D., Simmons, A., Soci, C., Dee, D., Thépaut, J-N. (2018). ERA5 hourly data on single levels from 1940 to present [data file]. Copernicus Climate Change Service (C3S) Climate Data Store (CDS) [producer]. DOI: 10.24381/cds.adbb2d47. Open-Meteo [distributor]. https://open-meteo.com/
- National Centers for Environmental Information. (2018). nClimDiv Temperature-Precipitation [data files]. National Centers for Environmental Information [producer]. National Oceanic and Atmospheric Administration [distributor]. https://www.ncei.noaa.gov/pub/data/cirs/climdiv/
- US Department of Interior Office of Wildland Fire: IRWIN, USDA Forest Service, National Park Service, Fish and Wildlife Service, Bureau of Indian Affairs, and Bureau of Land Management, National Association of State Foresters, US Fire Administration, National Wildfire Coordinating Group. (2014). Wildland Fire Incident Locations [data file]. Wildland Fire Interagency Geospatial Services [producer]. National Interagency Fire Center [distributor]. https://nifc.maps.arcgis.com/home/item.html?id=b4402f7887ca4ea9a6189443f220ef28

</details>
  
## License

[Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
