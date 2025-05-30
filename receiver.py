from flask import Flask, request, jsonify
import os
import subprocess
from werkzeug.utils import secure_filename
from datetime import datetime
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import json

app = Flask(__name__)

# Configuration
with open('config.json', 'r') as f:
    data = json.load(f)
    port = data['port']
    workers = data['workers']
    max_file = data['max_file']
    max_total = data['max_total']
    contact = data['contact']


UPLOAD_FOLDER = 'received_files'
ALLOWED_EXTENSIONS = {'mp4', 'mkv'}
supported_dbs = ["TheMovieDB", "OMDb", "TheTVDB", "AniDB", "TheMovieDB::TV", "TVmaze", "AcoustID", "ID3", "exif", "xattr", "file", "OpenSubtitles", "Shooter", "AnimeLists", "FanartTV"]
MAX_FILE_SIZE = max_file * 1024 * 1024  # 10MB per file
MAX_TOTAL_SIZE = max_total * 1024 * 1024  # 200MB total for all files
filebot = "filebot"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
if not os.path.exists('Logs'):
    os.makedirs('Logs')
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.chmod(app.config['UPLOAD_FOLDER'], 0o755)
#app.config['MAX_CONTENT_LENGTH'] = MAX_TOTAL_SIZE + (10 * 1024)

class APIError(Exception):
    """Custom API error class"""
    def __init__(self, message, status_code, error_type=None, payload=None):
        super().__init__()
        self.message = message
        self.status_code = status_code
        self.error_type = error_type or "api_error"
        self.payload = payload or {}
        self.timestamp = datetime.now()
        self.error_id = str(uuid.uuid4())

    def to_dict(self):
        error = {
            'error': {
                'id': self.error_id,
                'type': self.error_type,
                'message': self.message,
                'timestamp': self.timestamp,
                **self.payload
            }
        }
        with open(f"Logs/{self.error_id}.txt", 'w') as f:
            f.write(str(error))
        return error

@app.errorhandler(APIError)
def handle_api_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

def allowed_file(filename: str) -> bool:
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_new_filename(filepath: str, format: str, db: str, q: str, upload_folder: str) -> str:
    try:
        # Create a temporary output directory for this specific file
        original_filename = os.path.basename(filepath)
        temp_output_dir = os.path.join(upload_folder, f"temp_{os.path.splitext(original_filename)[0]}_{str(uuid.uuid4())}")
        os.makedirs(temp_output_dir, exist_ok=True)
        
        # Build the command
        if db:
            if db in supported_dbs:
                if q:
                    command = [filebot, '-rename', filepath, '--format', format, '--db', db, '--q', q, '--output', temp_output_dir]
                else:
                    command = [filebot, '-rename', filepath, '--format', format, '--db', db, '--output', temp_output_dir]
            else:
                raise APIError("This Database is not supported", 400, "invalid_database")
        else:
            command = [filebot, '-rename', filepath, '--format', format, '--output', temp_output_dir]
        
        # Run the command
        subprocess.run(command, check=True)
        
        # Get the renamed file from the temp directory
        renamed_files = os.listdir(temp_output_dir)
        if not renamed_files:
            raise APIError("No output file was created", 500, "file_processing_error")
        
        # Move the file to the main upload folder
        new_filepath = os.path.join(upload_folder, renamed_files[0])
        os.rename(os.path.join(temp_output_dir, renamed_files[0]), new_filepath)
        
        # Clean up the temp directory
        os.rmdir(temp_output_dir)
        
        return os.path.basename(new_filepath)
        
    except subprocess.CalledProcessError as e:
        # Clean up temp directory if it exists
        if 'temp_output_dir' in locals() and os.path.exists(temp_output_dir):
            for f in os.listdir(temp_output_dir):
                os.remove(os.path.join(temp_output_dir, f))
            os.rmdir(temp_output_dir)
        
        raise APIError(
            "File renaming failed",
            status_code=500,
            error_type="file_processing_error",
            payload={
                'command': ' '.join(command),
                'return_code': e.returncode
            }
        )
    except Exception as e:
        # Clean up temp directory if it exists
        if 'temp_output_dir' in locals() and os.path.exists(temp_output_dir):
            for f in os.listdir(temp_output_dir):
                os.remove(os.path.join(temp_output_dir, f))
            os.rmdir(temp_output_dir)
        raise

@app.route('/help', methods=['GET', 'POST'])
def help():
    return jsonify({
        'api': 'FileBot API',
        'version': '1.0',
        'endpoints': {
            '/rename': {
                'methods': ['POST'],
                'parameters': {
                    'files': {
                        'type': 'file[]',
                        'required': True,
                        'description': 'Video files to process'
                    },
                    'format': {
                        'type': 'string',
                        'required': True,
                        'description': 'FileBot naming format'
                    },
                    'db': {
                        'type': 'string',
                        'required': False,
                        'description': 'Chosen Database for naming'
                    },
                    'q': {
                        'type': 'string',
                        'required': False,
                        'description': 'Force lookup by search query or numeric ID'
                    }
                }
            },
            '/ping':{
                'methods': ['GET'],
                'parameters': 'None',
                'description': 'Used to test the connection'
            }
        },
        'limits': {
            'max_file_size': f"{MAX_FILE_SIZE//(1024*1024)}MB",
            'max_total_size': f"{MAX_TOTAL_SIZE//(1024*1024)}MB",
            'allowed_extensions': list(ALLOWED_EXTENSIONS)
        }
    })


@app.route('/ping', methods=['GET'])
def handle_ping():
    return jsonify({'status': 200, 'message': 'pong'})


@app.route('/rename', methods=['POST'])
def handle_callback():
    try:
        # Validate request
        if 'files' not in request.files:
            raise APIError("No files provided", 400, "missing_files")
        if 'format' not in request.form:
            raise APIError("No format provided", 400, "missing_format")

        files = request.files.getlist('files')
        format = request.form['format'].strip()
        db = request.form['db'] if 'db' in request.form else None
        q = request.form['q'] if 'q' in request.form else None


        if not files or any(f.filename == '' for f in files):
            raise APIError("No valid files selected", 400, "invalid_files")
        if not format:
            raise APIError("Format cannot be empty", 400, "invalid_format")
        
        upload_folder = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(str(datetime.now())))
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
            os.chmod(upload_folder, 0o755)

        processed_files = []
        total_size = 0


        for file in files:
            # Validate file
            if not allowed_file(file.filename):
                raise APIError(
                    f"Invalid file type: {file.filename}",
                    400,
                    "invalid_file_type",
                    {'allowed_types': list(ALLOWED_EXTENSIONS)}
                )

            # Check file size
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            if file_size > MAX_FILE_SIZE:
                raise APIError(
                    "File size exceeds limit",
                    413,
                    "file_too_large",
                    {
                        'filename': file.filename,
                        'actual_size': f"{file_size//(1024*1024)}MB",
                        'max_allowed_size': f"{MAX_FILE_SIZE//(1024*1024)}MB"
                    }
                )

            total_size += file_size
            if total_size > MAX_TOTAL_SIZE:
                raise APIError(
                    "Total size exceeds limit",
                    413,
                    "total_size_exceeded",
                    {
                        'current_total': f"{total_size//(1024*1024)}MB",
                        'max_allowed': f"{MAX_TOTAL_SIZE//(1024*1024)}MB"
                    }
                )
        
        
        def process_file(file, format, db, q, upload_folder):
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            
            original_filename = secure_filename(file.filename)
            file_path = os.path.join(upload_folder, original_filename)
            file.save(file_path)
            new_filename = ''
            
            try:
                new_filename = generate_new_filename(file_path, format, db, q, upload_folder)
                return {
                    'original_name': file.filename,  # Using file.filename instead of original_filename to preserve original
                    'new_name': new_filename,
                    'size': f"{file_size//1024}KB"
                }
            except Exception as e:
                print(f"Error processing {original_filename}: {str(e)}")
                # Clean up the original file if processing failed
                if os.path.exists(file_path):
                    os.remove(file_path)
                return None
            # Removed the finally block that was deleting files prematurely

        def process_files_with_executor(files, format, db, q, upload_folder, max_workers):
            processed_files = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks to the executor and keep track of file objects
                futures = []
                file_objects = []
                
                for file in files:
                    # Make a copy of the file object for each thread
                    file_copy = type(file)(file.stream, filename=file.filename, content_type=file.content_type)
                    futures.append(executor.submit(process_file, file_copy, format, db, q, upload_folder))
                    file_objects.append(file_copy)
                
                # Process results as they complete
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            processed_files.append(result)
                    except Exception as e:
                        print(f"Exception occurred: {str(e)}")
            
            # Clean up all files after all processing is done
            for file in file_objects:
                file_path = os.path.join(upload_folder, secure_filename(file.filename))
                if os.path.exists(file_path):
                    os.remove(file_path)
                new_file_path = os.path.join(upload_folder, secure_filename(file.filename) + ".converted")
                if os.path.exists(new_file_path):
                    os.remove(new_file_path)
            
            return processed_files


        processed_files = process_files_with_executor(
            files=files,
            format=request.form.get('format'),
            db=db,
            q=q,
            upload_folder=upload_folder,
            max_workers=workers  # Optimal for 12-thread CPU (I/O bound)
        )
        
        # os.remove(upload_folder)
        shutil.rmtree(upload_folder)
        id = str(uuid.uuid4())
        success = {
            'status': 'success',
            'id': id,
            'processed_files': processed_files,
            'total_size': f"{total_size//(1024*1024)}MB",
            'file_count': len(processed_files)
        }
        
        with open(f"Logs/{id}.txt", 'w') as f:
            f.write(str(success))

        return jsonify(success)

    except APIError:
        raise  # Re-raise our custom errors
    except Exception as e:
        raise APIError(
            str(e),
            500,
            "server_error",
            {'support_contact': contact}
        )

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    # from waitress import serve
    # serve(app, host="0.0.0.0", port=port)
    app.run(host='0.0.0.0', port=port, debug=True)