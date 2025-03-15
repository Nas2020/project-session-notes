# """
# Functions for processing text and HTML content.
# """
# import re
# from bs4 import BeautifulSoup

# def extract_text_from_html(html_content):
#     """
#     Extract text from HTML content and preserve paragraphs.
    
#     Args:
#         html_content (str): HTML content to process
        
#     Returns:
#         str: Plain text with preserved paragraph structure
#     """
#     if not html_content:
#         return ""
    
#     # Replace <br> tags with newlines before parsing
#     html_content = re.sub(r'<br\s*/?>', '\n', html_content)
    
#     soup = BeautifulSoup(html_content, 'html.parser')
    
#     # First, get direct text nodes (text not in any block elements)
#     paragraphs = []
    
#     # Extract text directly in the body before any block elements
#     direct_text = ''
#     for child in soup.children:
#         if isinstance(child, str) and child.strip():
#             direct_text += child.strip() + ' '
    
#     if direct_text.strip():
#         paragraphs.append(direct_text.strip())
    
#     # Then extract text from block elements
#     for element in soup.find_all(['p', 'div']):
#         text = element.get_text(strip=True)
#         if text:
#             paragraphs.append(text)
    
#     # Join paragraphs with double newlines for better readability
#     text = "\n\n".join(paragraphs)
    
#     # Clean up excessive whitespace
#     text = re.sub(r'\n{3,}', '\n\n', text)
#     text = re.sub(r' {2,}', ' ', text)
    
#     return text.strip()
#     """
#     Extract text from HTML content and preserve paragraphs.
    
#     Args:
#         html_content (str): HTML content to process
        
#     Returns:
#         str: Plain text with preserved paragraph structure
#     """
#     if not html_content:
#         return ""
    
#     # Replace <br> tags with newlines before parsing
#     html_content = re.sub(r'<br\s*/?>', '\n', html_content)
    
#     soup = BeautifulSoup(html_content, 'html.parser')
    
#     # Get the full text
#     full_text = soup.get_text()
    
#     # Split by newlines and keep non-empty lines
#     paragraphs = [line.strip() for line in full_text.split('\n') if line.strip()]
    
#     # Join paragraphs with double newlines for better readability
#     text = "\n\n".join(paragraphs)
    
#     # Clean up excessive whitespace
#     text = re.sub(r'\n{3,}', '\n\n', text)
#     text = re.sub(r' {2,}', ' ', text)
    
#     return text.strip()

"""
Functions for processing text and HTML content.
"""
import re
from bs4 import BeautifulSoup

def extract_text_from_html(html_content):
    """
    Extract plain text from HTML content:
    - Discards all HTML tags and non-text elements (e.g., images, styling, bullet points).
    - Normalizes all whitespace and newlines to a single space.
    - Removes special characters that might cause SQL syntax errors.
    - Escapes single quotes by replacing them with two single quotes.
    
    Args:
        html_content (str or bytes): The HTML content to process.
        
    Returns:
        str: The cleaned plain text, or an empty string if content is empty.
    """
    if not html_content:
        return ""
    
    # Convert to string if bytes
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8', errors='ignore')
    
    # Replace <br> tags with a space
    html_content = re.sub(r'<br\s*/?>', ' ', html_content)
    
    # Parse HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove image tags (or any other tags not needed)
    for img in soup.find_all('img'):
        img.decompose()
    
    # Extract text from HTML
    text = soup.get_text(separator=" ", strip=True)
    
    # Normalize newlines and extra whitespace to a single space
    text = re.sub(r'[\n\r]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    # Remove non-printable ASCII characters (keeps space to tilde)
    text = re.sub(r'[^\x20-\x7E]+', ' ', text)
    
    # Double-escape single quotes for SQL insertion (ensuring they're properly escaped)
    text = text.replace("'", "''")
    
    # Remove any characters that could cause SQL injection or parsing issues
    text = re.sub(r'[\\";`]', '', text)
    
    return text.strip()