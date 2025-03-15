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
    if not html_content:
        return ""
    
    # Replace <br> tags with newlines before parsing
    html_content = re.sub(r'<br\s*/?>', '\n', html_content)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # First, get direct text nodes (text not in any block elements)
    paragraphs = []
    
    # Extract text directly in the body before any block elements
    direct_text = ''
    for child in soup.children:
        if isinstance(child, str) and child.strip():
            direct_text += child.strip() + ' '
    
    if direct_text.strip():
        paragraphs.append(direct_text.strip())
    
    # Then extract text from block elements
    for element in soup.find_all(['p', 'div']):
        text = element.get_text(strip=True)
        if text:
            paragraphs.append(text)
    
    # Join paragraphs with double newlines for better readability
    text = "\n\n".join(paragraphs)
    text = re.sub(r'\n+', ' ', text)  # Collapse newlines to spaces
    text = re.sub(r' {2,}', ' ', text)
    text = text.replace("'", "''")
        
    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    
    # Escape single quotes for SQL
    text = text.replace("'", "''")
    
    return text.strip()