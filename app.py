#!/usr/bin/env python3
import os

import aws_cdk as cdk

from pedestrian_analysis.pedestrian_analysis_stack import PedestrianAnalysisStack


app = cdk.App()
PedestrianAnalysisStack(app, "PedestrianAnalysisStack")

app.synth()
