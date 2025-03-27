import sys
sys.path.append('./airflow')
import polars as pl
import requests
import logging
import time
from PIL import Image
from io import BytesIO
from datetime import datetime
from pyvi import ViTokenizer
from transformers import AutoTokenizer


class TextOperator():
    @staticmethod
    def tokenize_vietnamese(text: str):
        tokens = ViTokenizer.tokenize(text).split(" ")
        return tokens

    @staticmethod
    def scaling_data(df:pl.DataFrame, selected_columns:list=None):
        if selected_columns != None:
            temp_df = df.select(selected_columns)
        else:
            temp_df = df.select('*')
        return temp_df

    @staticmethod
    def clean_caption(df:pl.DataFrame):
        regex_pattern = r'[!“"”#$%&()*+,./:;<=>?@\[\\\]\^{|}~-]'
        cleaned_df = df.with_columns(
            pl.lit(datetime.now()).alias("created_time"),
            pl.col("short_caption").str.to_lowercase().alias("short_caption"),
        ).with_columns(
            pl.col("short_caption").str.replace_all(regex_pattern, "").alias("short_caption")
        )
        cleaned_df = cleaned_df.with_columns(
            tokenized_caption=pl.col("short_caption")
                                .map_elements(lambda x: TextOperator.tokenize_vietnamese(x), return_dtype=pl.List(pl.String()))
                                .list.join(" ")
        )
        return cleaned_df

    @staticmethod
    def perform_imc(image_path, url):
        response = requests.post(url=url, json={"image_url": image_path,})
        print("Response in = ", response.elapsed.total_seconds())
        if response.status_code == 200:
            return response.json().get("response_message")
        else:
            print("Error:", response.status_code, response.text)
            return None
        
    @staticmethod
    def encode_caption(caption:str):
        # Load tokenizer của BartPho
        tokenizer = AutoTokenizer.from_pretrained("vinai/bartpho-word")
        # encode caption
        tokenized = tokenizer.encode_plus(caption, return_tensors="pt", padding="max_length", truncation=True)
        tokenized_dict = dict(tokenized)
        return tokenized_dict

    @staticmethod
    def load_image_from_url(url):
        """Load image from URL with resize"""
        try:
            response = requests.get(url, timeout=10, verify=False)  # Bỏ qua SSL verify
            response.raise_for_status()
            image = Image.open(BytesIO(response.content))
            
            # Resize image if too large
            max_size = (800, 800)  # Giới hạn kích thước tối đa
            if image.size[0] > max_size[0] or image.size[1] > max_size[1]:
                image.thumbnail(max_size, Image.Resampling.LANCZOS)
                
            return image
        except Exception as e:
            print(f"Error loading image from URL: {e}")
            return None

    @staticmethod
    def caption_generator(genai, image_url, prompt, max_retries=3):
        """Get prediction with retries"""
        for attempt in range(max_retries):
            try:
                model = genai.GenerativeModel('gemini-2.0-flash')
                image = TextOperator.load_image_from_url(image_url)
                if image is None:
                    return None

                response = model.generate_content([prompt, image])
                # if response.text is None:
                #     raise Exception(f"Cannot generate caption for image url {image_url} at attempt {attempt}/{max_retries}")
                return response.text
                
            except Exception as e:
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 * (attempt + 1))
                


if __name__ == "__main__":
    import google.generativeai as genai
    from utils.setting import get_settings
    
    settings = get_settings()
    genai.configure(api_key=settings.GEMINI_API_KEY)
    
    # Bạn là một người đang trực tiếp tham gia giao thông tại Việt Nam. Hãy mô tả ngắn gọn tình huống giao thông hiện tại, tập trung vào các phương tiện chính, con người và môi trường xung quanh. Đảm bảo mô tả rõ ràng, dễ hiểu, không quá 25 từ, giúp người khiếm thị nhanh chóng hình dung bối cảnh giao thông.
    promt = """
        Bạn là một người khiếm thị đang nghe mô tả về tình huống giao thông xung quanh. Hãy mô tả thành đoạn văn ngắn gọn và khách quan các tiêu chí sau:  
        **Tình trạng giao thông**: 
            - Mô tả tình trạng giao thông hiện tại bằng một câu đơn, tập trung vào phương tiện chính, biển báo giao thông, đèn tín hiệu, con người và bối cảnh trong tấm hình.  
        **Vị trí các đối tượng cố định**: 
            - Chỉ rõ vị trí "trái", "phải", "phía trước", "chính giữa", "bên lề"... của các đối tượng cố định như biển báo, đèn tín hiệu, chốt cảnh sát...   
            - Nếu có các biển báo giao thông hoặc đèn tín hiệu giao thông, nêu rõ nội dung của chúng.
        **Làn đường và hướng di chuyển**: 
            - Chỉ rõ phương tiện di chuyển (cùng chiều hoặc ngược chiều) so với tôi (góc nhìn của ảnh).
            - Nếu phương tiện băng ngang, nêu rõ hướng di chuyển ("từ trái sang phải", "từ phải sang trái").
        **Góc nhìn trong ảnh**: 
            - Xác định vị trí hiện tại so với góc chụp ảnh (ví dụ: đứng trên vỉa hè, giữa đường, hay nhìn từ xa). Xưng hô "bạn".
        **Khả năng di chuyển an toàn**:
            - Xác định chính xác vị trí "trái", "phải", "phía trước" hoặc "chính giữa" cho làn đường có vỉa hè, vạch qua đường cho người đi bộ hoặc làn đường không có vật cản ảnh hưởng đến việc di chuyển an toàn.
        **Kèm theo các ràng buộc điều kiện sau**:
            - Chỉ sử dụng câu đơn có chủ ngữ, động từ, bổ ngữ rõ ràng. 
            - Sử dụng câu tự nhiên, có thể nói thành lời mà không gây khó hiểu
            - Không mô tả thông tin hiển nhiên.
            - Tách ý bằng dấu chấm (.) và không dùng dấu phẩy (,) hoặc chấm phẩy (;) để nối câu.
            - Không thêm cảm xúc hay suy đoán. Không quá 20 từ. Nếu dài hơn, hãy rút gọn.
            - Các câu phải nối tiếp nhau trên cùng một dòng, không xuống dòng mới.
    """
    caption = TextOperator.caption_generator(genai=genai, 
                                   image_url="https://laodongthudo.vn/stores/news_dataimages/quocdai/112016/18/09/2249_VHdibo-2.jpg",
                                   prompt=promt)
    print(caption)