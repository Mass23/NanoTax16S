# this will be the main script
import os
import argparse
import subprocess
import pandas as pd # type: ignore
import datetime
import glob

################################################################################
#################           FUNCTIONS           ################################
################################################################################
# Preprocessing part
def run_basecalling(input, output):
  args = f"dorado basecaller sup -r merged.pod5 --kit-name SQK-NBD114-96 > {os.path.join(output, 'raw_data', calls.bam)}"
  subprocess.call(args, shell = True)

def run_demux(output):
  args = f"dorado demux --output-dir {os.path.join(output, raw_data)} --emit-fastq --no-classify {os.path.join(output, 'raw_data', calls.bam)}"
  subprocess.call(args, shell = True)

def gzip_fastq(output):
  args = f"gzip {os.path.join(output, 'raw_data', '*.fastq')}"
  subprocess.call(args, shell = True)

def list_fastq_gz(output):
  fastq_gz_files = glob.glob(os.path.join(output, 'raw_data', '*.fastq.gz'))
  return(fastq_gz_files)

def run_porechop(samples, threads):
  for sample in samples:
    args = f"porechop --threads {str(threads)} -i {sample} -o {sample.replace('.fastq.gz', '_porechop.fastq.gz')}"
    subprocess.call(args, shell = True)

    with open(f'{results_folder_name}/log.txt', 'a') as log:
      log.write(args + '\n\n')

def run_chopper(samples, threads, qual_threshold):
  for sample in samples:
    reads_in = sample.replace('.fastq.gz', '_porechop.fastq.gz')
    reads_out = sample.replace('.fastq.gz', '_porechop.fastq.gz')
    args = f'gunzip -c {reads_in} | chopper -q {str(qual_threshold)} --maxlength {str(1800)} --minlength {str(1200)} --threads {str(threads)} | gzip > {reads_out}'
    subprocess.call(args, shell = True)
    subprocess.call(f'rm {reads_in}', shell = True)

    with open(f'{results_folder_name}/log.txt', 'a') as log:
      log.write(args + ' and deleted intermediary porechop file.' + '\n\n')

def preprocess(input, output, threads, qual_threshold):
  run_basecalling(input, output)
  run_demux(output)
  gzip_fastq(output)
  fastq_gz_files = list_fastq_gz(output)
  run_porechop(fastq_gz_files, threads)
  run_chopper(fastq_gz_files, threads, qual_threshold)
  
# Clustering
#def cluster_otus()

# Taxonomy

################################################################################
#################           MAIN                ################################
################################################################################
def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Help:")

    # Add the folder path argument
    parser.add_argument("-i", "--input", type=str,
                        help="Path to the pod5 file", required=True)
    parser.add_argument("-o", "--output", type=str,
                        help="Name of the results folder (_results will be added at the end)", required=True)
    parser.add_argument("-t", "--threads", type=str,
                        help="Number of threads to use for multiprocessing-compatible tasks", required=True)
    parser.add_argument("--skippreprocessing", action='store_true',
                        help="To add if you want to skip preprocessing")
    parser.add_argument("--skipqiime2", action='store_true',
                        help="To add if you want to skip the qiime2 part (only preprocessing)")
    parser.add_argument("--perc_identity", action='store_true', default='0.97',
                        help="To add if you want to skip the qiime2 part (only preprocessing)")
    parser.add_argument("--qual_threshold", action='store_true', default='12',
                        help="Quality threshold for the reads preprocessing")

    # Parse arguments
    args = parser.parse_args()
    out_folder = f'{args.name}_results'
    os.makedirs(out_folder)
    
    if args.skippreprocessing is False:
        preprocess(args.input, args.output, args.threads, args.qual_threshold)

    # cluster_otus(args.output, args.threads, args.perc_identity)

main()


