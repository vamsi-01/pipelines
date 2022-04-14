# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import click
import kfp
from kfp.cli.output import OutputFormat
from kfp.client import Client

PROGRAM_NAME = 'kfp'
SHELL_FILES = {
    'bash': ['.bashrc'],
    'zsh': ['.zshrc'],
    'fish': ['.config', 'fish', 'completions', f'{PROGRAM_NAME}.fish']
}


def _create_completion(shell: str) -> str:
    return f'eval "$(_{PROGRAM_NAME.upper()}_COMPLETE={shell}_source {PROGRAM_NAME})"'


def _show_completion(shell: str) -> None:
    click.echo(_create_completion(shell))


def _install_completion(shell: str) -> None:
    completion_statement = _create_completion(shell)
    source_file = os.path.join(os.path.expanduser('~'), *SHELL_FILES[shell])
    print('SOURCE FILE: ', source_file)
    with open(source_file, 'a') as f:
        f.write(completion_statement + '\n')


@click.group(invoke_without_command=True)
@click.option(
    '--show-completion',
    type=click.Choice(list(SHELL_FILES.keys())),
    default=None)
@click.option(
    '--install-completion',
    type=click.Choice(list(SHELL_FILES.keys())),
    default=None)
@click.option('--endpoint', help='Endpoint of the KFP API service to connect.')
@click.option('--iap-client-id', help='Client ID for IAP protected endpoint.')
@click.option(
    '-n',
    '--namespace',
    default='kubeflow',
    show_default=True,
    help='Kubernetes namespace to connect to the KFP API.')
@click.option(
    '--other-client-id',
    help='Client ID for IAP protected endpoint to obtain the refresh token.')
@click.option(
    '--other-client-secret',
    help='Client ID for IAP protected endpoint to obtain the refresh token.')
@click.option(
    '--output',
    type=click.Choice(list(map(lambda x: x.name, OutputFormat))),
    default=OutputFormat.table.name,
    show_default=True,
    help='The formatting style for command output.')
@click.pass_context
@click.version_option(version=kfp.__version__, message='%(prog)s %(version)s')
def cli(ctx: click.Context, endpoint: str, iap_client_id: str, namespace: str,
        other_client_id: str, other_client_secret: str, output: OutputFormat,
        show_completion: str, install_completion: str):
    """kfp is the command line interface to KFP service.

    Feature stage:
    [Alpha](https://github.com/kubeflow/pipelines/blob/07328e5094ac2981d3059314cc848fbb71437a76/docs/release/feature-stages.md#alpha)
    """
    if show_completion:
        _show_completion(show_completion)
        return
    if install_completion:
        _install_completion(install_completion)
        return

    NO_CLIENT_COMMANDS = ['diagnose_me', 'components']
    if ctx.invoked_subcommand in NO_CLIENT_COMMANDS:
        # Do not create a client for these subcommands
        return
    ctx.obj['client'] = Client(endpoint, iap_client_id, namespace,
                               other_client_id, other_client_secret)
    ctx.obj['namespace'] = namespace
    ctx.obj['output'] = output
