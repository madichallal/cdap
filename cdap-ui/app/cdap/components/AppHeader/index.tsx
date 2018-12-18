/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import * as React from 'react';
import PropTypes from 'prop-types';
import AppBar from '@material-ui/core/AppBar';
import classnames from 'classnames';
import AppDrawer from 'components/AppHeader/AppDrawer/AppDrawer';
import AppToolbar from 'components/AppHeader/AppToolBar/AppToolbar';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import MuiThemeProvider from '@material-ui/core/styles/MuiThemeProvider';
import createMuiTheme, { ThemeOptions } from '@material-ui/core/styles/createMuiTheme';
import { MyNamespaceApi } from 'api/namespace';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import ee from 'event-emitter';
import { ISubscription } from 'rxjs/Subscription';
import { Unsubscribe } from 'redux';
import globalEvents from 'services/global-events';
import getLastSelectedNamespace from 'services/get-last-selected-namespace';
import { SYSTEM_NAMESPACE } from 'services/global-constants';
import { objectQuery } from 'services/helpers';
import { NamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';

require('styles/bootstrap_4_patch.scss');

interface IMyAppHeaderState {
  toggleDrawer: boolean;
  currentNamespace: string;
}
const styles = (theme) => {
  return {
    grow: theme.grow,
    appbar: {
      backgroundColor: theme.palette.primary.main,
      zIndex: theme.zIndex.drawer + 1,
    },
  };
};

interface IMyAppHeaderProps extends WithStyles<typeof styles> {
  nativeLink: boolean;
}

class MyAppHeader extends React.PureComponent<IMyAppHeaderProps, IMyAppHeaderState> {
  public state: IMyAppHeaderState = {
    toggleDrawer: false,
    currentNamespace: '',
  };

  private namespacesubscription: ISubscription;
  private nsSubscription: Unsubscribe;
  private eventEmitter = ee(ee);

  public componentWillMount() {
    // Polls for namespace data
    this.namespacesubscription = MyNamespaceApi.pollList().subscribe((res) => {
      if (res.length > 0) {
        NamespaceStore.dispatch({
          type: NamespaceActions.updateNamespaces,
          payload: {
            namespaces: res,
          },
        });
      } else {
        // TL;DR - This is emitted for Authorization in main.js
        // This means there is no namespace for the user to work on.
        // which indicates she/he have no authorization for any namesapce in the system.
        this.eventEmitter.emit(globalEvents.NONAMESPACE);
      }
    });
    this.nsSubscription = NamespaceStore.subscribe(() => {
      let selectedNamespace: string = getLastSelectedNamespace() as string;
      const { namespaces } = NamespaceStore.getState();
      if (selectedNamespace === SYSTEM_NAMESPACE) {
        selectedNamespace = objectQuery(namespaces, 0, 'name');
      }
      if (selectedNamespace !== this.state.currentNamespace) {
        this.setState({
          currentNamespace: selectedNamespace,
        });
      }
    });
  }
  public componentWillUnmount() {
    this.nsSubscription();
    if (this.namespacesubscription) {
      this.namespacesubscription.unsubscribe();
    }
  }

  public toggleDrawer = () => {
    this.setState({
      toggleDrawer: !this.state.toggleDrawer,
    });
  };

  public render() {
    const { classes } = this.props;
    const namespaceLinkContext = {
      namespace: this.state.currentNamespace,
      isNativeLink: this.props.nativeLink,
    };
    return (
      <AppBar position="fixed" className={classnames(classes.grow, classes.appbar)}>
        <NamespaceLinkContext.Provider value={namespaceLinkContext}>
          <AppToolbar onMenuIconClick={this.toggleDrawer} nativeLink={this.props.nativeLink} />
          <AppDrawer
            open={this.state.toggleDrawer}
            onClose={this.toggleDrawer}
            componentDidNavigate={this.toggleDrawer}
          />
        </NamespaceLinkContext.Provider>
      </AppBar>
    );
  }
}
const AppHeaderWithStyles = withStyles(styles)(MyAppHeader);
const baseTheme = createMuiTheme({
  palette: {
    primary: {
      main: '#1a73e8',
    },
    blue: {
      50: '#045599',
      100: '#0076dc',
      200: '#0099ff',
      300: '#58b7f6',
      400: '#7cd2eb',
      500: '#cae7ef',
    },
  },
  buttonLink: {
    '&:hover': {
      color: 'inherit',
      backgroundColor: 'rgba(255, 255, 255, 0.10)',
    },
    fontSize: '1rem',
    color: 'white',
  },
  iconButtonFocus: {
    '&:focus': {
      outline: 'none',
      backgroundColor: 'rgba(255, 255, 255, 0.10)',
    },
  },
  grow: {
    flexGrow: 1,
  },
  typography: {
    fontSize: 13,
    fontFamily: 'var(--font-family)',
    useNextVariants: true,
  },
} as ThemeOptions);
/**
 *  We are adding the themeing only to the header. Two reasons,
 *
 * 1. Right now only the header is slowly transitioning to material design
 * 2. Header needs to be shared across react and angular and if I add MuiThemeProvider
 *   to main.js angular side won't get the theme. Trying to avoid duplicating theme between angular
 *   and react since we are anways moving away from angular.
 */
export default function CustomHeader({ nativeLink }) {
  return (
    <MuiThemeProvider theme={baseTheme}>
      <AppHeaderWithStyles nativeLink={nativeLink} />
    </MuiThemeProvider>
  );
}
// Apparently this is needed for ngReact
(CustomHeader as any).propTypes = {
  nativeLink: PropTypes.bool,
};
(CustomHeader as any).defaultProps = {
  nativeLink: false,
};
