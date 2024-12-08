import argparse
from argparse import Namespace

from google.cloud import aiplatform
import google.auth

INSTANCES = [
    {
        'dep_delay': 16.0,
        'taxiout': 13.0,
        'distance': 160.0,
        'avg_dep_delay': 13.34,
        'avg_arr_delay': 67.0,
        'carrier': 'AS',
        'dep_lat': 61.17,
        'dep_lon': -150.00,
        'arr_lat': 60.49,
        'arr_lon': -145.48,
        'origin': 'ANC',
        'dest': 'CDV'
    }
]

def arg_parse() -> Namespace:
    """

    :return:
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--location',
        type=str
    )

    parser.add_argument(
        '--endpoint_id',
        type=str
    )

    return parser.parse_args()

def main():
    parser: Namespace = arg_parse()

    credential, project_id = google.auth.default()

    # Ref
    # https://cloud.google.com/vertex-ai/docs/predictions/get-online-predictions#predict-request
    aiplatform.init(
        project=project_id,
        location=parser.location,
        credentials=credential)

    ep: aiplatform.Endpoint = aiplatform.Endpoint(
        endpoint_name=parser.endpoint_id,
    )

    resp: aiplatform.models.Prediction = ep.predict(instances=INSTANCES)

    # TODO: Implement explanation
    # https://cloud.google.com/vertex-ai/docs/explainable-ai/overview
    #resp: aiplatform.models.Prediction = ep.explain(
    #    instances=INSTANCES)
    #
    #for explanation in resp.explanations:
    #    print(" explanation")
    #    # Feature attributions.
    #    attributions = explanation.attributions
    #    for attribution in attributions:
    #        print("  attribution")
    #        print("   baseline_output_value:", attribution.baseline_output_value)
    #        print("   instance_output_value:", attribution.instance_output_value)
    #        print("   output_display_name:", attribution.output_display_name)
    #        print("   approximation_error:", attribution.approximation_error)
    #        print("   output_name:", attribution.output_name)
    #        output_index = attribution.output_index
    #        for output_index in output_index:
    #            print("   output_index:", output_index)

    print(f'model_id: {resp.deployed_model_id}, prediction: {resp.predictions}')

if __name__ == "__main__":
    main()
