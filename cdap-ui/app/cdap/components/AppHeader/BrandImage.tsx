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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { Theme } from 'services/ThemeHelper';

const BrandImage: React.SFC<WithStyles<typeof imageStyle>> = ({ classes }) => {
  const brandLogoSrc = Theme.productLogoNavbar || '/cdap_assets/img/company_logo.png';
  return <img className={classes.img} src={brandLogoSrc} />;
};

const imageStyle = {
  img: {
    width: '108px',
    height: '50px',
  },
};
const StyledBrandImage = withStyles(imageStyle)(BrandImage);
export default StyledBrandImage;
