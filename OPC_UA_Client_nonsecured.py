import pandas as pd
from opcua import Client
from datetime import datetime
import time
import os

url = "opc.tcp://192.168.0.1:4840"  # Declaring the URL for connection

client = Client(url)

# Declaring the dictionary to hold variables - key: value
variables = {
    'SW_Data_1': 'ns=3;s="SW_Module"."Data_1"',
    'SW_Data_2': 'ns=3;s="SW_Module"."Data_2"',
    'SW_Data_3': 'ns=3;s="SW_Module"."Data_3"',
    'SW_Data_4': 'ns=3;s="SW_Module"."Data_4"',
    'SW_Data_5': 'ns=3;s="SW_Module"."Data_5"',
    'DPW_Data_1': 'ns=3;s="DPW_Module"."Data_1"',
    'DPW_Data_2': 'ns=3;s="DPW_Module"."Data_2"',
    'DPW_Data_3': 'ns=3;s="DPW_Module"."Data_3"',
    'DPW_Data_4': 'ns=3;s="DPW_Module"."Data_4"',
    'DPW_Data_5': 'ns=3;s="DPW_Module"."Data_5"',
    'AGW_Data_1': 'ns=3;s="AGW_Module"."Data_1"',
    'AGW_Data_2': 'ns=3;s="AGW_Module"."Data_2"',
    'AGW_Data_3': 'ns=3;s="AGW_Module"."Data_3"',
    'AGW_Data_4': 'ns=3;s="AGW_Module"."Data_4"',
    'AGW_Data_5': 'ns=3;s="AGW_Module"."Data_5"'
}

# Initialize an empty list to store data
data = []

# Initialize a dictionary to store the last recorded value for each variable
last_values = {}

# Dictionary to track last row index for each container's dataframe
last_row_indices = {}


try:
    client.connect()  # Creating client object
    while True:
        try:
            for var, node_id in variables.items():  # Loop through the dictionary
                node = client.get_node(node_id)  # Retrieve the node ID
                value = node.get_value()  # Retrieve the value of the node ID
                timestamp = datetime.now()

                # Extract container name from the node_id
                container_name = node_id.split('"')[1]

                # Check if the variable's value has changed
                if var not in last_values or last_values[var] != value:
                    # Update the last recorded value
                    last_values[var] = value

                    # Append the current record to the data list
                    data.append(
                        {'Timestamp': timestamp, 'Variable': var, 'Value': value, 'container_Name': container_name})

            # Convert the data list to a dataframe
            df = pd.DataFrame(data)

            # Filter the dataframe by unique container names
            unique_containers = df['container_Name'].unique()

            for container in unique_containers:
                # Filter data for the current container
                container_df = df[df['container_Name'] == container]

                # Get unique variables within the container
                unique_variables = container_df['Variable'].unique()

                for variable in unique_variables:
                    # Filter by both container and variable
                    filtered_df = container_df[container_df['Variable'] == variable]

                    # Define the Excel file name
                    file_name = f"{container}.xlsx"
                    # Get the last row index of the filtered dataframe
                    current_row_index = filtered_df.index[-1]
                    # Check if the Excel file exists
                    if os.path.exists(file_name):
                        # Load the existing Excel file
                        existing_df = pd.read_excel(file_name)

                        # Filter the existing Excel file by the same container and variable
                        filtered_existing_df = existing_df[
                            (existing_df['container_Name'] == container) & (existing_df['Variable'] == variable)
                            ]

                        # Compare the last rows of the filtered dataframes
                        if not filtered_existing_df.empty:
                            last_row_existing = filtered_existing_df.iloc[-1]
                            last_row_filtered = filtered_df.iloc[-1]
                           # print(filtered_existing_df)

                            if last_row_existing['Value'] == last_row_filtered['Value']:
                                print(f"No updates needed for {file_name}")
                                continue  # Skip updating if the last rows are identical

                        # Append only new rows
                       # if container not in last_row_indices or current_row_index > last_row_indices[container]:
                            # Append only new rows
                          #  new_rows = filtered_df.iloc[last_row_indices.get(variable, -1) + 1:]
                            last_row_filtered = filtered_df.iloc[[-1]]
                            updated_df = pd.concat([existing_df, last_row_filtered]).drop_duplicates().reset_index(
                            drop=True)

                            # Save the updated dataframe to Excel
                            updated_df.to_excel(file_name, index=False)
                         #   last_row_indices[variable] = current_row_index
                    else:
                        # Create new Excel file with filtered data
                       container_df.to_excel(file_name, index=False)
                       last_row_indices[variable] = current_row_index

            print(df)  # Display the dataframe

            time.sleep(2)

        except KeyboardInterrupt:
            print("Program interrupted by user")
            break

        except Exception as e:
            print("Error in reading node:", e)

except Exception as e:
    print("Unable to connect to OPC UA server:", e)

finally:
    client.disconnect()
