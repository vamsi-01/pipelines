import os


def convert_rst_to_md(content):
    """
    Convert RST links to Markdown links.
    Example:
        `Link text <http://example.com>`_ -> [Link text](http://example.com)
    """
    import re
    return re.sub(r'`([^`]+) <([^>]+)>`_', r'[\1](\2)', content)


def process_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    modified_content = convert_rst_to_md(content)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)


def main(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                process_file(file_path)


if __name__ == '__main__':
    project_directory = './google_cloud_pipeline_components'
    main(project_directory)
