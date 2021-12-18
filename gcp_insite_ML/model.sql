create model `advde_ds_us.model`
	options (model_type="boosted_tree_classifier",
		max_iterations=10, input_label_cols=["species"])
	as SELECT
		*
	FROM
		`bigquery-public-data.ml_datasets.iris`;