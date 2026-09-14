"""Configuration for the GB CTR prediction export."""

from dataclasses import dataclass

import numpy as np

from bigquery_etl.newtab_merino.ctrpred.infer_ctr import ActrSsmConfig
from bigquery_etl.newtab_merino.ctrpred.pseudo_counts import PseudoCountConfig


@dataclass(frozen=True)
class CtrPredConfig:
    actr: ActrSsmConfig
    pseudo_counts: PseudoCountConfig


GB_CTRPRED_CONFIG = CtrPredConfig(
    actr=ActrSsmConfig(
        global_ctr=0.00549142617968888,
        hourly_ctr=np.array(
            [
                0.0037897577120721564,
                0.0035874066829025995,
                0.0036730439822962816,
                0.004293027405310703,
                0.005289842319061653,
                0.006487817232205769,
                0.006947288889418829,
                0.00678856494788479,
                0.006775118639931629,
                0.006349082720865816,
                0.006132815554137066,
                0.006021984817201792,
                0.005620085412221571,
                0.005481977400190812,
                0.005543267234804683,
                0.005578956877204055,
                0.005454693303285034,
                0.0048683316001689935,
                0.004597954489191772,
                0.004499000427179724,
                0.004454265152352611,
                0.004432251843976086,
                0.004281685879836372,
                0.004073081680422711,
            ]
        ),
        item_prior_exposure=430.3858471754073,
        phi=0.7786195907676119,
        process_variance=0.07397743278083943,
        initial_variance=0.9194228740523188,
    ),
    pseudo_counts=PseudoCountConfig(strength_multiplier=32.0),
)
