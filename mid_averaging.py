from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow as tf
import numpy as np
import keras
from keras.models import load_model

def mid_averaging(updates, model_path, ckpt_path):

    print("Model path: ", model_path)
    print("Checkpoint path: ", ckpt_path)
    print("Updates array: ", updates)

    total_num_batches = 0

    # Load model architecture
    sum_weight_updates = load_model(model_path)
    device_weight_updates = load_model(model_path)

    # Calculate sum of weight updates from all devices
    for device_index in range(len(updates)):

        n, weight_updates_path = updates[device_index]

        total_num_batches += n

        if device_index == 0:
            sum_weight_updates.load_weights(weight_updates_path)

        else:
            # Load device weight updates checkpoint
            device_weight_updates.load_weights(weight_updates_path)

            # Add weight updates from device to prefix sum
            for layer_index in range(len(sum_weight_updates.layers)):

                # Old sum of weight updates
                old_sum_weight_updates_values = sum_weight_updates.layers[layer_index].get_weights(
                )

                # Device weight updates
                device_weight_updates_values = device_weight_updates.layers[layer_index].get_weights(
                )

                # Weight updates calculation
                sum_weight_updates.layers[layer_index].set_weights(np.asarray(old_sum_weight_updates_values)
                                                                   + np.asarray(device_weight_updates_values))

#                print("old weights: ",  old_layer_weights)
#                print("new weights: ",  new_layer_weights)
#                print("update weights: ",  update_weights.layers[i].get_weights())

    # Add average of weight updates to checkpoint
    # Load model and checkpoints
    model = load_model(model_path)

    for layer_index in range(len(model.layers)):

        # weight sum updates values
        sum_weight_updates_values = sum_weight_updates.layers[layer_index].get_weights(
        )

        model.layers[layer_index].set_weights(np.asarray(sum_weight_updates_values))

    # Save updated model checkpoints
    model.save_weights(ckpt_path)
    print("New checkpoint saved at ", ckpt_path)

def main():
        # define Arguments
    parser = argparse.ArgumentParser(description='Perform Federated Averaging')
    parser.add_argument("--cf", "--ckpt-file-path", required=True, nargs=1)
    parser.add_argument("--mf", "--model-file-path", required=True, nargs=1)
    parser.add_argument("--u", "--updates", required=True, nargs='*')

    # params for federated averaging
    model_path = ''
    ckpt_path = ''
    updates = []

    # parse arguments
    args = parser.parse_args()
    for arg in vars(args):
                # print(arg, getattr(args, arg))
        if (arg == "mf"):
            model_path = getattr(args, arg)[0]
        elif (arg == "cf"):
            ckpt_path = getattr(args, arg)[0]
        else:
            update_args = getattr(args, arg)
            print(update_args)
            print("Len: ", len(update_args))
            for i in range(0, len(update_args), 2):
                print("index: ", i)
                # print(update_args[i])
                # print(update_args[i + 1])
                n = int(update_args[i])
                path = update_args[i + 1]
                updates.append((n, path))

    # run federated averaging
    mid_averaging(updates, model_path, ckpt_path)


if __name__ == "__main__":
    main()
