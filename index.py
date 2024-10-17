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

def mapper(data):
    """Chuyển đổi dữ liệu thành cặp khóa-giá trị cho MapReduce."""
    for index, row in data.iterrows():
        yield row['anime_id'], row.to_dict()

def reducer_union(values):
    """Kết hợp các bản ghi và loại bỏ các bản ghi trùng lặp dựa trên id."""
    unique_records = {}
    for key, value in values:
        unique_records[key] = value
    return list(unique_records.values())

def reducer_intersection(values1, values2):
    """Giữ lại các bản ghi xuất hiện trong cả hai file."""
    set1 = {v['anime_id']: v for v in values1}
    set2 = {v['anime_id']: v for v in values2}
    intersection = [set1[key] for key in set1 if key in set2]
    return intersection

def reducer_difference(values1, values2):
    """Giữ lại các bản ghi chỉ xuất hiện trong file thứ hai."""
    set1 = {v['anime_id']: v for v in values1}
    set2 = {v['anime_id']: v for v in values2}
    difference = [set2[key] for key in set2 if key not in set1]
    return difference

def main():
    input_hdfs_path1 = "/anime/anime.csv"
    input_hdfs_path2 = "/anime/anime2.csv"
    output_hdfs_path_sort = "/anime/kq_sort.csv"
    output_hdfs_path_union = "/anime/kq_union.csv"
    output_hdfs_path_intersection = "/anime/kq_intersection.csv"
    output_hdfs_path_difference = "/anime/kq_difference.csv"
    local_download_path_sort = "kq_sort.csv"
    local_download_path_union = "kq_union.csv"
    local_download_path_intersection = "kq_intersection.csv"
    local_download_path_difference = "kq_difference.csv"

    try:
        # Đọc dữ liệu từ HDFS
        data1 = read_data_from_hdfs(input_hdfs_path1)
        data2 = read_data_from_hdfs(input_hdfs_path2)

        #sort
        data = read_data_from_hdfs(input_hdfs_path1)
        mapped_data = list(mapper_sort(data))
        sorted_data = reducer_sort([value for _, value in mapped_data])
        sorted_df = pd.DataFrame(sorted_data)
        sorted_df.to_csv(local_download_path_sort, index=False)
        write_data_to_hdfs(output_hdfs_path_sort, local_download_path_sort)
        download_file_from_hdfs(output_hdfs_path_sort, local_download_path_sort)
        
        # Union
        mapped_data1 = list(mapper(data1))
        mapped_data2 = list(mapper(data2))
        combined_data = mapped_data1 + mapped_data2
        unique_data = reducer_union(combined_data)
        united_df = pd.DataFrame(unique_data)
        united_df.to_csv(local_download_path_union, index=False)
        write_data_to_hdfs(output_hdfs_path_union, local_download_path_union)
        download_file_from_hdfs(output_hdfs_path_union, local_download_path_union)

        # Intersection
        intersection_data = reducer_intersection([v[1] for v in mapped_data1], [v[1] for v in mapped_data2])
        intersection_df = pd.DataFrame(intersection_data)
        intersection_df.to_csv(local_download_path_intersection, index=False)
        write_data_to_hdfs(output_hdfs_path_intersection, local_download_path_intersection)
        download_file_from_hdfs(output_hdfs_path_intersection, local_download_path_intersection)

        # Difference
        difference_data = reducer_difference([v[1] for v in mapped_data1], [v[1] for v in mapped_data2])
        difference_df = pd.DataFrame(difference_data)
        difference_df.to_csv(local_download_path_difference, index=False)
        write_data_to_hdfs(output_hdfs_path_difference, local_download_path_difference)
        download_file_from_hdfs(output_hdfs_path_difference, local_download_path_difference)

    except Exception as e:
        print(f"Đã xảy ra lỗi: {str(e)}")

if __name__ == "__main__":
    main()