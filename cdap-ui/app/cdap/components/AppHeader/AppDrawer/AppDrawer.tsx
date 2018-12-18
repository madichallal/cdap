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
import List from '@material-ui/core/List';
import ListItemText from '@material-ui/core/ListItemText';
import Drawer from '@material-ui/core/Drawer';
import NamespaceDropdown from 'components/NamespaceDropdown';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
const colorVariables = require('styles/variables.scss');
import { Link } from 'react-router-dom';
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import ListItemLink from 'components/AppHeader/ListItemLink';
import DrawerFeatureLink from 'components/AppHeader/AppDrawer/DrawerFeatureLink';
import { Theme } from 'services/ThemeHelper';

const DRAWER_WIDTH = '250px';
const styles = (theme) => {
  return {
    drawer: {
      zIndex: theme.zIndex.drawer,
      width: DRAWER_WIDTH,
    },
    drawerPaper: {
      width: DRAWER_WIDTH,
      backgroundColor: colorVariables.grey08,
    },
    listItemText: {
      fontWeight: 600,
      fontSize: '1rem',
    },
    toolbar: theme.mixins.toolbar,
  };
};

interface IAppDrawerProps extends WithStyles<typeof styles> {
  open: boolean;
  onClose: () => void;
  componentDidNavigate: () => void;
  context: INamespaceLinkContext;
}

class AppDrawer extends React.PureComponent<IAppDrawerProps> {
  public state = {
    onNamespacePreferenceEdit: false,
  };
  public toggleonNamespacePreferenceEdit = () => {
    this.setState({ onNamespacePreferenceEdit: !this.state.onNamespacePreferenceEdit });
  };
  public render() {
    const { classes, open, onClose, componentDidNavigate = () => null } = this.props;
    const { isNativeLink, namespace } = this.props.context;
    return (
      <Drawer
        open={open}
        onClose={onClose}
        className={classes.drawer}
        disableEnforceFocus={true}
        disableEscapeKeyDown={this.state.onNamespacePreferenceEdit}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.drawerPaper,
        }}
      >
        <div className={classes.toolbar} />
        <NamespaceDropdown
          onNamespaceCreate={onClose}
          onNamespacePreferenceEdit={this.toggleonNamespacePreferenceEdit}
          tag={isNativeLink ? 'a' : Link}
        />
        <List component="nav" dense={true}>
          <ListItemLink
            component={isNativeLink ? 'a' : Link}
            href={`/cdap/ns/${namespace}`}
            to={`/ns/${namespace}`}
            onClick={componentDidNavigate}
          >
            <ListItemText
              disableTypography
              classes={{ root: classes.listItemText }}
              primary="Control Center"
            />
          </ListItemLink>
        </List>
        <List component="nav" dense={true}>
          <DrawerFeatureLink
            featureName={Theme.featureNames.pipelines}
            featureFlagName={Theme.showPipelines}
            featureUrl={`/pipelines/ns/${namespace}`}
            isAngular={true}
          />
          <DrawerFeatureLink
            featureName={Theme.featureNames.dataPrep}
            featureFlagName={Theme.showDataPrep}
            featureUrl={`/ns/${namespace}/dataprep`}
            componentDidNavigate={this.props.componentDidNavigate}
          />
          <DrawerFeatureLink
            featureUrl={`/ns/${namespace}/experiments`}
            featureName={Theme.featureNames.analytics}
            featureFlagName={Theme.showAnalytics}
            componentDidNavigate={this.props.componentDidNavigate}
          />
          <DrawerFeatureLink
            featureUrl={`/ns/${namespace}/rulesengine`}
            featureName={Theme.featureNames.rulesEngine}
            featureFlagName={Theme.showRulesEngine}
            componentDidNavigate={this.props.componentDidNavigate}
          />
          <DrawerFeatureLink
            featureName={Theme.featureNames.metadata}
            featureFlagName={Theme.showMetadata}
            featureUrl={`/metadata/ns/${namespace}`}
            isAngular={true}
          />
        </List>
        <List component="nav" dense={true}>
          <ListItemLink
            component={isNativeLink ? 'a' : Link}
            href="/administration/configuration"
            to="/cdap/administration/configuration"
            onClick={componentDidNavigate}
          >
            <ListItemText
              disableTypography
              classes={{ root: classes.listItemText }}
              primary="Project Admin"
            />
          </ListItemLink>
        </List>
      </Drawer>
    );
  }
}

export default withStyles(styles)(withContext(AppDrawer));
