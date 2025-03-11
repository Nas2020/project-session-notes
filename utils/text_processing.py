"""
Functions for processing text and HTML content.
"""
import re
from bs4 import BeautifulSoup


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
    
#     soup = BeautifulSoup(html_content, 'html.parser')
#     paragraphs = []
#     for p in soup.find_all(['p', 'div']):
#         text = p.get_text(strip=True)
#         if text:
#             paragraphs.append(text)
    
#     # Join paragraphs with double newlines for better readability
#     text = "\n\n".join(paragraphs)
    
#     # Clean up excessive whitespace
#     text = re.sub(r'\n{3,}', '\n\n', text)
#     text = re.sub(r' {2,}', ' ', text)
    
#     return text.strip()

def extract_text_from_html(html_content):
    """
    Extract text from HTML content and preserve paragraphs.
    
    Args:
        html_content (str): HTML content to process
        
    Returns:
        str: Plain text with preserved paragraph structure
    """
    if not html_content:
        return ""
    
    # Replace <br> tags with newlines before parsing
    html_content = re.sub(r'<br\s*/?>', '\n', html_content)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Get the full text
    full_text = soup.get_text()
    
    # Split by newlines and keep non-empty lines
    paragraphs = [line.strip() for line in full_text.split('\n') if line.strip()]
    
    # Join paragraphs with double newlines for better readability
    text = "\n\n".join(paragraphs)
    
    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    
    return text.strip()