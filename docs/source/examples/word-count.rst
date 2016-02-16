Word count
==========

Setup
-----

In this example, we'll use the ``hdfs3`` library to count the number of words
in text files (Enron email dataset, 6.4 GB) stored in HDFS.

Copy the text data from Amazon S3 into HDFS on the cluster:

.. code-block:: bash

   $ hadoop distcp s3n://AWS_SECRET_ID:AWS_SECRET_KEY@blaze-data/enron-email hdfs:///tmp/enron

where ``AWS_SECRET_ID`` and ``AWS_SECRET_KEY`` are valid AWS credentials.

Code example
------------

Import ``hdfs3`` and other standard libraries used in this example:

.. code-block:: python

   >>> import hdfs3
   >>> from collections import defaultdict, Counter

Initalize a connection to HDFS, replacing ``NAMENODE_HOSTNAME`` and
``NAMENODE_PORT`` with the hostname and port (default: 8020) of the HDFS
namenode.

.. code-block:: python

   >>> hdfs = hdfs3.HDFileSystem('NAMENODE_HOSTNAME', port=NAMENODE_PORT)

Generate a list of filenames from the text data in HDFS:

.. code-block:: python

   >>> filenames = hdfs.glob('/tmp/enron/*/*')
   >>> print(filenames[:5])

   ['/tmp/enron/edrm-enron-v2_nemec-g_xml.zip/merged.txt',
   '/tmp/enron/edrm-enron-v2_ring-r_xml.zip/merged.txt',
   '/tmp/enron/edrm-enron-v2_bailey-s_xml.zip/merged.txt',
   '/tmp/enron/edrm-enron-v2_fischer-m_xml.zip/merged.txt',
   '/tmp/enron/edrm-enron-v2_geaccone-t_xml.zip/merged.txt']

Print the first 1024 bytes of the first text file:

.. code-block:: python

   >>> print(hdfs.head(filenames[0]))

   b'Date: Wed, 29 Nov 2000 09:33:00 -0800 (PST)\r\nFrom: Xochitl-Alexis Velasc
   o\r\nTo: Mark Knippa, Mike D Smith, Gerald Nemec, Dave S Laipple, Bo Barnwel
   l\r\nCc: Melissa Jones, Iris Waser, Pat Radford, Bonnie Shumaker\r\nSubject:
    Finalize ECS/EES Master Agreement\r\nX-SDOC: 161476\r\nX-ZLID: zl-edrm-enro
   n-v2-nemec-g-2802.eml\r\n\r\nPlease plan to attend a meeting to finalize the
    ECS/EES  Master Agreement \r\ntomorrow 11/30/00 at 1:30 pm CST.\r\n\r\nI wi
   ll email everyone tomorrow with location.\r\n\r\nDave-I will also email you 
   the call in number tomorrow.\r\n\r\nThanks\r\nXochitl\r\n\r\n***********\r\n
   EDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL
    Technologies, Inc. This Data Set is licensed under a Creative Commons Attri
   bution 3.0 United States License <http://creativecommons.org/licenses/by/3.0
   /us/> . To provide attribution, please cite to "ZL Technologies, Inc. (http:
   //www.zlti.com)."\r\n***********\r\nDate: Wed, 29 Nov 2000 09:40:00 -0800 (P
   ST)\r\nFrom: Jill T Zivley\r\nTo: Robert Cook, Robert Crockett, John Handley
   , Shawna'

Create a function to count words in each file:

.. code-block:: python

   >>> def count_words(file):
   ...     word_counts = defaultdict(int)
   ...     for line in file:
   ...         for word in line.split():
   ...             word_counts[word] += 1
   ...     return word_counts

   >>> print(count_words(['apple banana apple', 'apple orange']))

   defaultdict(int, {'apple': 3, 'banana': 1, 'orange': 1})

Count the number of words in the first text file:

.. code-block:: python

   >>> with hdfs.open(filenames[0]) as f:
   ...     counts = count_words(f)
   
   >>> print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   [(b'the', 1065320),
    (b'of', 657220),
    (b'to', 569076),
    (b'and', 545821),
    (b'or', 375132),
    (b'in', 306271),
    (b'shall', 255680),
    (b'be', 210976),
    (b'any', 206962),
    (b'by', 194780)]

Count the number of words in all of the text files. This operation required
about 10 minutes to run on a single machine with 4 cores and 16 GB RAM:

.. code-block:: python

   >>> all_counts = Counter()
   >>> for fn in filenames:
   ...     with hdfs.open(fn) as f:
   ...         counts = count_words(f)
   ...         all_counts.update(counts)

Print the total number of words and the words with the highest frequency from
all of the text files:

.. code-block:: python

   >>> print(len(all_counts))

   8797842

   >>> print(sorted(all_counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   [(b'0', 67218380),
    (b'the', 19586868),
    (b'-', 14123768),
    (b'to', 11893464),
    (b'N/A', 11814665),
    (b'of', 11724827),
    (b'and', 10253753),
    (b'in', 6684937),
    (b'a', 5470371),
    (b'or', 5227805)]

The complete Python script for this example is shown below:

.. code-block:: python

   # word-count.py   
   
   import hdfs3
   from collections import defaultdict, Counter

   hdfs = hdfs3.HDFileSystem('NAMENODE_HOSTNAME', port=NAMENODE_PORT)
   
   filenames = hdfs.glob('/tmp/enron/*/*')
   print(filenames[:5])
   print(hdfs.head(filenames[0]))
   
   
   def count_words(file):
       word_counts = defaultdict(int)
       for line in file:
           for word in line.split():
               word_counts[word] += 1
       return word_counts
   
   print(count_words(['apple banana apple', 'apple orange']))
   
   with hdfs.open(filenames[0]) as f:
       counts = count_words(f)
   
   print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])
   
   all_counts = Counter()
   
   for fn in filenames:
       with hdfs.open(fn) as f:
           counts = count_words(f)
           all_counts.update(counts)
   
   print(len(all_counts))
   print(sorted(all_counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])
