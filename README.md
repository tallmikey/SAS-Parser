# SAS-Parser

SAS Program Parser to Display the Data Workflow Implied

.. pseudocode::

    - Open .sas files
    - Ignore comments
    - Identify Input
    - Identify Output
    - Link input and output together
    - Create a schema to represent the workflow and export the flow to .dot file
    - Export the input/output to csv files

.. warning::

    Assume that program parsed can be run without error

.. DONE:: adjust consistency among extracted components
.. DONE:: identify Macro variable input and output. 
.. todo:: identify PROCs/DATA STEPs run thru multiple lines
.. todo:: identify data flows when macro calls involved.

.. Bugs::
.. Data step output pickup extra data step statements
