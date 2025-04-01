import pandas as pd

input_file = 'IM_DU_30D+_NON_sessioners_1743455715_gaid'  
output_file = 'IM_DU_30D+_NON_sessioners_1743455715_gaid.csv'
chunk_size = 999999 
separator = ',' 

chunks = []
try:
    for chunk in pd.read_csv(input_file, sep=separator, chunksize=chunk_size):
        chunks.append(chunk)

    data = pd.concat(chunks, axis=0)

    data.to_csv(output_file, index=False)
    print(f"Data successfully converted to CSV and saved to {output_file}")
except Exception as e:
    print(f"Error while processing and saving the data: {e}")

try:
    chunk_iter = pd.read_csv(output_file, chunksize=chunk_size)

    for i, chunk in enumerate(chunk_iter):
        output_filename = f'IM_DU_30D+_NON_sessioners_1741550619_gaid{i + 1}.csv'
        chunk.to_csv(output_filename, index=False)
        print(f'Split {i + 1} saved as {output_filename}')

    print("Splitting completed!")
except Exception as e:
    print(f"Error while splitting the data: {e}")
