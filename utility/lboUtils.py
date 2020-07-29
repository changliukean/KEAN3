import pandas as pd
import sys










def read_excel_lbo_inputs(file_path, load_fsli_list):

    raw_lbo_inputs_df = pd.DataFrame()
    for load_fsli in load_fsli_list:
        fsli_tab_name = "Value_" + "_".join(load_fsli.split(" "))
        print (fsli_tab_name)
        first_cell_name = "Output_" + "_".join(load_fsli.split(" "))
        temp_raw_fsli_inputs_df = pd.read_excel(file_path, sheet_name=fsli_tab_name)
        temp_raw_fsli_inputs_df.rename(columns={'Unnamed: 1':'unit', first_cell_name:'entity'}, inplace=True)
        temp_raw_fsli_inputs_df = temp_raw_fsli_inputs_df.iloc[3:]
        melted_raw_fsli_inputs_df = pd.melt(temp_raw_fsli_inputs_df,
                                            id_vars=['entity','unit'],
                                            value_vars=[item for item in list(temp_raw_fsli_inputs_df.columns) if item != 'entity' and item != 'unit'],
                                            var_name='period',
                                            value_name='value')
        melted_raw_fsli_inputs_df['fsli'] = load_fsli

        raw_lbo_inputs_df = raw_lbo_inputs_df.append(melted_raw_fsli_inputs_df)

    return raw_lbo_inputs_df
