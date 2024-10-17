from hdfs import InsecureClient
import pandas as pd
import io

def read_data_from_hdfs(hdfs_path):
    client = InsecureClient('http://localhost:9870', user='dr.who')
    with client.read(hdfs_path) as reader:
        data = pd.read_csv(io.BytesIO(reader.read()))
        return data

def write_data_to_hdfs(hdfs_path, local_path):
    client = InsecureClient('http://localhost:9870', user='dr.who')
    client.upload(hdfs_path, local_path, overwrite=True)
    print(f"Đã lưu tệp từ {local_path} lên {hdfs_path}")

def download_file_from_hdfs(hdfs_path, local_path):
    client = InsecureClient('http://localhost:9870', user='dr.who')
    client.download(hdfs_path, local_path, overwrite=True)
    print(f"Đã tải tệp từ {hdfs_path} về {local_path}")

def mapper_sort(data):
    """Chuyển đổi dữ liệu thành cặp khóa-giá trị cho MapReduce."""
    for index, row in data.iterrows():
        yield row['rating'], row

def reducer_sort(sorted_values):
    """Giảm giá trị và sắp xếp theo điểm."""
    return sorted(sorted_values, key=lambda x: x['rating'])

# Union (hop)
def mapper(data):
    """Chuyển đổi dữ liệu thành cặp khóa-giá trị cho MapReduce."""
    for index, row in data.iterrows():
        print(f"Mapping row: {row.to_dict()}")
        yield row['anime_id'], row.to_dict()

def reducer(values):
    """Kết hợp các bản ghi và loại bỏ các bản ghi trùng lặp dựa trên nội dung tất cả các cột."""
    unique_records = []
    seen_records = set()

    for value in values:
        record_tuple = tuple(value[1].items())
        if record_tuple not in seen_records:
            seen_records.add(record_tuple)
            unique_records.append(value[1])

    return unique_records

def mapper_insection(data):
    """Chuyển đổi dữ liệu thành cặp khóa-giá trị cho MapReduce."""
    for index, row in data.iterrows():
        yield row['anime_id'], row.to_dict()

def reducer_insection(values1, values2):
    """Kết hợp các bản ghi và chỉ giữ lại các bản ghi trùng lặp dựa trên anime_id."""
    set1 = {tuple(value.items()) for value in values1}
    set2 = {tuple(value.items()) for value in values2}
    intersection = set1.intersection(set2)
    return [dict(item) for item in intersection]

def mapper_difference(data):
    """Chuyển đổi dữ liệu thành cặp khóa-giá trị cho MapReduce."""
    for index, row in data.iterrows():
        yield row['anime_id'], row.to_dict()

def reducer_difference(values1, values2):
    """Kết hợp các bản ghi và chỉ giữ lại các bản ghi có trong values1 nhưng không có trong values2 dựa trên anime_id."""
    set1 = {tuple(value.items()) for value in values1}
    set2 = {tuple(value.items()) for value in values2}
    difference = set2.difference(set1)
    return [dict(item) for item in difference]

def main():
    input_hdfs_path1 = "/anime/anime.csv"
    input_hdfs_path2 = "/anime/anime2.csv"
    output_hdfs_path_u = "/anime/kq_union.csv"
    output_hdfs_path_s = "/anime/kq_sort.csv"
    output_hdfs_path_d = "/anime/kq_diff.csv"
    output_hdfs_path_i = "/anime/kq_insec.csv"
    local_download_path_union = "kq_union.csv"
    local_download_path_sort = "kq_sort.csv"
    local_download_path_insec = "kq_insec.csv"
    local_download_path_diff = "kq_diff.csv"

    try:
        # Bước 1: Đọc dữ liệu từ HDFS
        data1 = read_data_from_hdfs(input_hdfs_path1)
        data2 = read_data_from_hdfs(input_hdfs_path2)

        #sort
        # Bước 2: Map
        mapped_data = list(map(lambda x: x, mapper_sort(data1)))

        # Bước 3: Reduce
        sorted_data = reducer_sort([value for _, value in mapped_data])

        # Chuyển đổi danh sách kết quả thành DataFrame
        sorted_df = pd.DataFrame(sorted_data)
        
        sorted_df.to_csv(local_download_path_sort, index=False)
        write_data_to_hdfs(output_hdfs_path_s, local_download_path_sort)
        download_file_from_hdfs(output_hdfs_path_s, local_download_path_sort)
        
        #Union
        # print(f"Data1:\n{data1}")
        # print(f"Data2:\n{data2}")

        # Bước 2: Map
        mapped_data1 = list(mapper(data1))
        mapped_data2 = list(mapper(data2))

        # print(f"Mapped Data1: {mapped_data1}")
        # print(f"Mapped Data2: {mapped_data2}")

        # Bước 3: Reduce
        combined_data = mapped_data1 + mapped_data2
        unique_data = reducer(combined_data)

        # In ra dữ liệu sau khi giảm
        # print(f"Unique Data: {unique_data}")

        # Chuyển đổi danh sách kết quả thành DataFrame
        united_df = pd.DataFrame(unique_data)
        
        # Intersection
        mapped_data3 = list(mapper_insection(data1))
        mapped_data4 = list(mapper_insection(data2))

        # Bước 3: Reduce
        intersection_data = reducer_insection([value for _, value in mapped_data3], [value for _, value in mapped_data4])

        # print(f"Intersection Data: {intersection_data}")

        # Chuyển đổi danh sách kết quả thành DataFrame
        intersection_df = pd.DataFrame(intersection_data)

        # Difference
        mapped_data5 = list(mapper_difference(data1))
        mapped_data6 = list(mapper_difference(data2))

        # Bước 3: Reduce
        difference_data = reducer_difference([value for _, value in mapped_data5], [value for _, value in mapped_data6])

        # print(f"Difference Data: {difference_data}")

        # Chuyển đổi danh sách kết quả thành DataFrame
        difference_df = pd.DataFrame(difference_data)

        # Bước 4: Ghi dữ liệu đã sắp xếp vào HDFS
        difference_df.to_csv(local_download_path_diff, index=False)
        write_data_to_hdfs(output_hdfs_path_d, local_download_path_diff)

        # Tải file kết quả về máy
        download_file_from_hdfs(output_hdfs_path_d, local_download_path_diff)
        print(f"Đã tải tệp {local_download_path_diff} về máy để kiểm tra.")
        
        # Bước 4: Ghi dữ liệu đã sắp xếp vào HDFS
        intersection_df.to_csv(local_download_path_insec, index=False)
        write_data_to_hdfs(output_hdfs_path_i, local_download_path_insec)

        # Tải file kết quả về máy
        download_file_from_hdfs(output_hdfs_path_i, local_download_path_insec)
        print(f"Đã tải tệp {local_download_path_insec} về máy để kiểm tra.")

        # Bước 4: Ghi dữ liệu đã sắp xếp vào HDFS
        united_df.to_csv(local_download_path_union, index=False)
        write_data_to_hdfs(output_hdfs_path_u, local_download_path_union)

        # Tải file kết quả về máy
        download_file_from_hdfs(output_hdfs_path_u, local_download_path_union)
        print(f"Đã tải tệp {local_download_path_union} về máy để kiểm tra.")
        
    except Exception as e:
        print(f"Đã xảy ra lỗi: {str(e)}")

if __name__ == "__main__":
    main()